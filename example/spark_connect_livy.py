from __future__ import annotations

import time
from enum import Enum
from typing import Optional

import botocore.session
import httpx
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from pydantic import BaseModel, Field


class SparkSessionList(BaseModel):
    from_: int = Field(alias="from")  # 'from' is a reserved keyword in Python
    total: int
    sessions: list[SparkSession]


class SparkSession(BaseModel):
    id: int
    name: Optional[str]
    appId: str
    owner: str
    proxyUser: Optional[str]
    state: str
    kind: str
    appInfo: dict
    log: list[str]
    ttl: Optional[int]
    driverMemory: Optional[str]
    driverCores: int
    executorMemory: Optional[str]
    executorCores: int
    conf: dict
    archives: list[str]
    files: list[str]
    heartbeatTimeoutInSecond: int
    jars: list[str]
    numExecutors: int
    pyFiles: list[str]
    queue: Optional[str]


class SparkStatementState(Enum):
    """Statement State
    | Value | Description |
    |-------|-------------|
    | waiting |Statement is enqueued but execution hasn't started|
    | running |Statement is currently running|
    | available |Statement has a response ready|
    | error |Statement failed|
    | cancelling |Statement is being cancelling|
    | cancelled |Statement is cancelled|
    """

    WAITING = "waiting"
    RUNNING = "running"
    AVAILABLE = "available"  # Terminal state
    ERROR = "error"  # Terminal state
    CANCELLING = "cancelling"
    CANCELLED = "cancelled"  # Terminal state

    def is_terminal(self) -> bool:
        return self in {
            SparkStatementState.AVAILABLE,
            SparkStatementState.ERROR,
            SparkStatementState.CANCELLED,
        }


class SparkStatement(BaseModel):
    id: int  ## The statement id
    code: str  ## The execution code
    state: SparkStatementState  ## The execution state
    output: Optional[dict]  ## The execution output
    progress: Optional[float]  ## The execution progress
    started: Optional[int]  ## The start time of statement code
    completed: Optional[int]  ## The complete time of statement code


class BotoSigV4AuthHttpx(httpx.Auth):
    def __init__(self, service, session=None, region_name="ap-southeast-1"):
        self.service = service
        self.session = session or botocore.session.Session()
        self.region_name = region_name
        self.sign_required_headers = [
            "content-type",
            "host",
            "x-amz-date",
            "x-amz-security-token",
        ]

    def auth_flow(self, request: httpx.Request):
        aws_signer = SigV4Auth(
            self.session.get_credentials(), self.service, self.region_name
        )
        aws_headers = {
            k: v
            for k, v in request.headers.items()
            if k.lower() in self.sign_required_headers
        }
        aws_request = AWSRequest(
            method=request.method,
            url=str(request.url),
            headers=aws_headers,
            data=request.content,
        )
        aws_signer.add_auth(aws_request)
        aws_prepared = aws_request.prepare()
        request.url = httpx.URL(aws_prepared.url)
        request.headers.update(aws_prepared.headers)
        yield request


class EMRPySparkLivyClient:
    def __init__(self, application_id: str, region: str = "ap-southeast-1"):
        self.endpoint = f"https://{application_id}.livy.emr-serverless-services.{region}.amazonaws.com"
        self.client = httpx.Client(
            auth=BotoSigV4AuthHttpx(
                "emr-serverless", botocore.session.Session(), region_name=region
            ),
            headers={"Content-Type": "application/json"},
        )

    def list_sessions(self):
        response = self.client.get(url=self.endpoint + "/sessions")
        if response.status_code != 200:
            raise Exception(f"Error listing sessions: {response.text}")
        return SparkSessionList.model_validate_json(response.text)

    def get_session(self, session_id: int):
        response = self.client.get(url=self.endpoint + f"/sessions/{session_id}")
        if response.status_code != 200:
            raise Exception(f"Error getting session {session_id}: {response.text}")
        return SparkSession.model_validate_json(response.text)

    def get_session_by_name(self, session_name: str):
        sessions = self.list_sessions()
        for session in sessions.sessions:
            if session.name == session_name:
                return session
        return None

    def delete_session(self, session_id: int):
        response = self.client.delete(url=self.endpoint + f"/sessions/{session_id}")
        if response.status_code != 200:
            raise Exception(f"Error deleting session {session_id}: {response.text}")
        return response.json()

    def create_session(
        self, execution_role_arn: str, name: str, spark_configs: dict = {}
    ):
        data = {
            "kind": "sql",
            "name": name,
            "heartbeatTimeoutInSecond": 60,
            "conf": {
                "emr-serverless.session.executionRoleArn": execution_role_arn,
                **spark_configs,
            },
        }
        response = self.client.post(url=self.endpoint + "/sessions", json=data)
        if response.status_code != 201:
            raise Exception(f"Error creating session: {response.text}")
        return SparkSession.model_validate_json(response.text)

    def submit_statement(self, session: SparkSession, code: str):
        """Submit a statement to a given session, Endpoint: POST /sessions/<sessionId>/statements
        Args:
            session (SparkSession): The Spark session to submit the statement to
            code (str): The code to execute
        Returns:
            SparkStatement: The submitted statement
        """
        data = {"code": code}
        response = self.client.post(
            url=self.endpoint + f"/sessions/{session.id}/statements", json=data
        )
        if response.status_code != 201:
            raise Exception(
                f"Error submitting statement to session {session.id}: {response.text}"
            )
        return SparkStatement.model_validate_json(response.text)

    def get_statement(self, session: SparkSession, statement_id: int):
        response = self.client.get(
            url=self.endpoint + f"/sessions/{session.id}/statements/{statement_id}"
        )
        if response.status_code != 200:
            raise Exception(
                f"Error getting statement {statement_id} from session {session.id}: {response.text}"
            )
        return SparkStatement.model_validate_json(response.text)


REGION = "ap-southeast-1"
# APPLICATION_ID = "00g1c8etjoblge25"
APPLICATION_ID = "00g19130rcrgsb25"
# EXECUTION_ROLE_ARN = "arn:aws:iam::474668419121:role/iam-role-dev-pvc-data-etl-job"
EXECUTION_ROLE_ARN = "arn:aws:iam::302010997939:role/pvcom-emr-serverless-role"

livy_client = EMRPySparkLivyClient(application_id=APPLICATION_ID, region=REGION)
print(livy_client.list_sessions())
# livy_client.delete_session(4)
# print("Creating new session...")
# for session in livy_client.list_sessions().sessions:
#     livy_client.delete_session(session.id)
exit(0)
session = livy_client.get_session_by_name("thanh-tran-dbt")
if session:
    print("Found session with id ", session.id)
else:
    print("No existing session found, creating new one...")
    session = livy_client.create_session(
        execution_role_arn=EXECUTION_ROLE_ARN,
        name="thanh-tran-dbt",
        spark_configs={
            "spark.driver.cores": "1",
            "spark.driver.memory": "2g",
            "spark.executor.cores": "2",
            "spark.executor.memory": "5g",
        },
    )
    print("New session created: ", session)
    # Wait for session to be ready
    while session.state != "idle":
        print("Session state: ", session.state)
        time.sleep(5)
        session = livy_client.get_session(session.id)

exit(0)
print("Submitting statement...")
statement = livy_client.submit_statement(
    session,
    code="""SELECT * FROM spark_catalog.demo_glue_db.sales_parquet_2""",
)
print("Submitted statement with id ", statement.id)
# wait for statement to complete
while not statement.state.is_terminal():
    print("Statement state: ", statement.state, statement.progress)
    time.sleep(2)
    statement = livy_client.get_statement(session, statement.id)
print("Final statement state: ", statement.state)
print("Statement output: ", statement.output)

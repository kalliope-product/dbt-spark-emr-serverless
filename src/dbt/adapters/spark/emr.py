"""Spark LIVY integration."""

from __future__ import annotations

import atexit
import datetime as dt
import os
import time
from copy import deepcopy
from enum import Enum
from types import TracebackType
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

import botocore.session
import httpx
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from dbt_common.exceptions import DbtRuntimeError
from dbt_common.utils.encoding import DECIMALS
from pydantic import BaseModel, Field

from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.spark.connections import SparkConnectionWrapper

logger = AdapterLogger("Spark")
NUMBERS = DECIMALS + (int, float)


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
        header_dicts = {k: v for k, v in aws_prepared.headers.items()}
        request.headers.update(header_dicts)
        yield request


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
            logger.error(f"Error listing sessions: {response.text}")
            raise Exception(f"Error listing sessions: {response.text}")
        return SparkSessionList.model_validate_json(response.text)

    def get_session(self, session_id: int):
        response = self.client.get(url=self.endpoint + f"/sessions/{session_id}")
        if response.status_code != 200:
            logger.error(f"Error getting session {session_id}: {response.text}")
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
            logger.error(f"Error deleting session {session_id}: {response.text}")
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
            logger.error(f"Error creating session: {response.text}")
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
            logger.error(
                f"Error submitting statement to session {session.id}: {response.text}"
            )
            raise Exception(
                f"Error submitting statement to session {session.id}: {response.text}"
            )
        return SparkStatement.model_validate_json(response.text)

    def get_statement(self, session: SparkSession, statement_id: int):
        response = self.client.get(
            url=self.endpoint + f"/sessions/{session.id}/statements/{statement_id}"
        )
        if response.status_code != 200:
            logger.error(
                f"Error getting statement {statement_id} from session {session.id}: {response.text}"
            )
            raise Exception(
                f"Error getting statement {statement_id} from session {session.id}: {response.text}"
            )
        return SparkStatement.model_validate_json(response.text)


class Cursor:
    """
    Mock a pyodbc cursor.

    Source
    ------
    https://github.com/mkleehammer/pyodbc/wiki/Cursor
    """

    def __init__(self, *, session: SparkSession, client: EMRPySparkLivyClient) -> None:
        self._schema = None
        self._rows = None
        self.session = session
        self.client = client

    def __enter__(self) -> Cursor:
        return self

    def __exit__(
        self,
        exc_type: Optional[BaseException],
        exc_val: Optional[Exception],
        exc_tb: Optional[TracebackType],
    ) -> bool:
        self.close()
        return True

    @property
    def description(
        self,
    ) -> Sequence[
        Tuple[
            str, Any, Optional[int], Optional[int], Optional[int], Optional[int], bool
        ]
    ]:
        """
        Get the description.

        Returns
        -------
        out : Sequence[Tuple[str, str, None, None, None, None, bool]]
            The description.

        Source
        ------
        https://github.com/mkleehammer/pyodbc/wiki/Cursor#description
        """
        if self._schema is None:
            description = list()
        else:
            description = [
                (
                    field["name"],
                    field["type"],
                    None,
                    None,
                    None,
                    None,
                    field["nullable"],
                )
                for field in self._schema.get("fields", [])
            ]
        return description

    def close(self) -> None:
        """
        Close the connection.

        Source
        ------
        https://github.com/mkleehammer/pyodbc/wiki/Cursor#close
        """
        self._schema = None
        self._rows = None

    def execute(self, sql: str, *parameters: Any) -> None:
        """
        Execute a sql statement.

        Parameters
        ----------
        sql : str
            Execute a sql statement.
        *parameters : Any
            The parameters.

        Raises
        ------
        NotImplementedError
            If there are parameters given. We do not format sql statements.

        Source
        ------
        https://github.com/mkleehammer/pyodbc/wiki/Cursor#executesql-parameters
        """
        if len(parameters) > 0:
            sql = sql % parameters

        # {
        #     "status": "ok",
        #     "execution_count": 0,
        #     "data": {
        #         "application/json": {
        #             "schema": {
        #                 "type": "struct",
        #                 "fields": [
        #                     {
        #                         "name": "id",
        #                         "type": "string",
        #                         "nullable": True,
        #                         "metadata": {},
        #                     },
        #                     {
        #                         "name": "value",
        #                         "type": "float",
        #                         "nullable": True,
        #                         "metadata": {},
        #                     },
        #                 ],
        #             },
        #             "data": [["2", 3.0], ["1", 1.0]],
        #         }
        #     },
        # }
        # Submit the statement
        statement = self.client.submit_statement(self.session, sql)
        # Wait for the statement to complete
        while not statement.state.is_terminal():
            logger.debug(
                f"Waiting for statement {statement.id} in session {self.session.id} to complete, current state: {statement.state.value}"
            )
            time.sleep(2)
            statement = self.client.get_statement(self.session, statement.id)
        if statement.state == SparkStatementState.ERROR:
            logger.error(
                f"Statement {statement.id} in session {self.session.id} failed with output: {statement.output}"
            )
            raise DbtRuntimeError(
                f"Statement {statement.id} in session {self.session.id} failed: {statement.output}"
            )
        # Parse the output
        if statement.output:
            output_data = statement.output.get("data", {}).get("application/json", {})
            self._schema = output_data.get("schema", {})
            self._rows = output_data.get("data", [])

    def fetchall(self) -> Optional[List]:
        """
        Fetch all data.

        Returns
        -------
        out : Optional[List[Row]]
            The rows.

        Source
        ------
        https://github.com/mkleehammer/pyodbc/wiki/Cursor#fetchall
        """
        return self._rows

    def fetchone(self) -> Any:
        """
        Fetch the first output.

        Returns
        -------
        out : Row | None
            The first row.

        Source
        ------
        https://github.com/mkleehammer/pyodbc/wiki/Cursor#fetchone
        """
        if self._rows is not None and len(self._rows) > 0:
            row = self._rows.pop(0)
        else:
            row = None

        return row


class Connection:
    """
    Mock a pyodbc connection.

    Source
    ------
    https://github.com/mkleehammer/pyodbc/wiki/Connection
    """

    def __init__(
        self,
        *,
        client: EMRPySparkLivyClient,
        session: SparkSession,
    ) -> None:
        self._client = client
        self._session = session
        self._cursor = Cursor(session=session, client=client)

    def cursor(self) -> Cursor:
        """
        Get a cursor.

        Returns
        -------
        out : Cursor
            The cursor.
        """
        return Cursor(session=self._session, client=self._client)


class EMRConnectionManager:
    def __init__(self, application_id: str):
        self.livy_global_session = None
        self._client = EMRPySparkLivyClient(application_id=application_id)
        atexit.register(self.__exit__)

    def __exit__(self):
        # Close the cursor and connection
        logger.info("Closing global EMR Serverless session...")
        if self.livy_global_session:
            self._client.delete_session(self.livy_global_session.id)
            logger.debug(
                f"Deleted global EMR Serverless session with id {self.livy_global_session.id}"
            )
        return True

    def connect(self, server_side_parameters: Optional[Dict[Any, str]] = None, session_name: str = "dbt-spark-livy-global-session") -> Connection:
        params = deepcopy(server_side_parameters) if server_side_parameters else {}
        if self.livy_global_session is None:
            execution_role_arn = params.pop(
                "emr-serverless.session.executionRoleArn", ""
            )
            if not execution_role_arn:
                logger.error(
                    "`emr-serverless.session.executionRoleArn` is required in server_side_parameters when using EMRServerless method to connect to Spark"
                )
                raise DbtRuntimeError(
                    "`emr-serverless.session.executionRoleArn` must be set in server_side_parameters when using EMRServerless method to connect to Spark"
                )
            session = self._client.create_session(
                execution_role_arn=execution_role_arn,
                name=os.getenv(
                    "DBT_SPARK_LIVY_SESSION_NAME", session_name
                ),
                spark_configs=params,
            )
            # Wait for session to be ready
            while session.state not in ("idle", "error", "dead"):
                logger.debug(
                    f"Waiting for global session {session.id} to be ready, current state: {session.state}"
                )
                time.sleep(5)
                session = self._client.get_session(session.id)
            if session.state != "idle":
                logger.error(
                    f"Global session {session.id} is in state {session.state}, cannot proceed"
                )
                raise DbtRuntimeError(
                    f"Global session {session.id} is in state {session.state}, cannot proceed"
                )
            logger.info(f"Created new global LIVY session with id {session.id}")
            self.livy_global_session = session
        else:
            logger.info(
                f"Reusing existing global LIVY session with id {self.livy_global_session.id}"
            )
        return Connection(client=self._client, session=self.livy_global_session)


class EMRConnectionWrapper(SparkConnectionWrapper):
    """Connection wrapper for the session connection method."""

    handle: Connection
    _cursor: Optional[Cursor]

    def __init__(self, handle: Connection) -> None:
        self.handle = handle
        self._cursor = None

    def cursor(self) -> "EMRConnectionWrapper":
        self._cursor = self.handle.cursor()
        return self

    def cancel(self) -> None:
        logger.debug("NotImplemented: cancel")

    def close(self) -> None:
        if self._cursor:
            self._cursor.close()

    def rollback(self, *args: Any, **kwargs: Any) -> None:
        logger.debug("NotImplemented: rollback")

    def fetchall(self) -> Optional[List]:
        assert self._cursor, "Cursor not available"
        return self._cursor.fetchall()

    def execute(self, sql: str, bindings: Optional[List[Any]] = None) -> None:
        if sql.strip().endswith(";"):
            sql = sql.strip()[:-1]

        assert self._cursor, "Cursor not available"
        if bindings is None:
            self._cursor.execute(sql)
        else:
            bindings = [self._fix_binding(binding) for binding in bindings]
            self._cursor.execute(sql, *bindings)

    @property
    def description(
        self,
    ) -> Sequence[
        Tuple[
            str, Any, Optional[int], Optional[int], Optional[int], Optional[int], bool
        ]
    ]:
        assert self._cursor, "Cursor not available"
        return self._cursor.description

    @classmethod
    def _fix_binding(cls, value: Any) -> Union[str, float]:
        """Convert complex datatypes to primitives that can be loaded by
        the Spark driver"""
        if isinstance(value, NUMBERS):
            return float(value)
        elif isinstance(value, dt.datetime):
            return f"'{value.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}'"
        else:
            return f"'{value}'"

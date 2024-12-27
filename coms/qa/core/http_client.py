import logging
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlunparse

import aiohttp
import requests
from requests.cookies import RequestsCookieJar
from requests.structures import CaseInsensitiveDict

__all__ = ["HttpClient", "HttpClientError", "SyncHttpClient", "AsyncHttpClient"]

logger = logging.getLogger(__name__)


class HttpClientError(Exception):
    def __init__(self, code: int, content: Any) -> None:
        super().__init__(f"{self.__class__.__name__}(code={code}, content={content})")

        self.code = code
        self.content = content


class HttpClient:
    """Base HTTP Client class"""

    def __init__(
        self, host: str, port: int = 80, scheme: str = "http", headers: Optional[Dict[str, Any]] = None
    ) -> None:
        self._host = host
        self._port = port
        self._scheme = scheme
        self._headers = headers or {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    @property
    def host(self) -> str:
        return self._host

    @property
    def port(self) -> int:
        return self._port

    @property
    def headers(self) -> Dict[str, Any]:
        return self._headers

    @headers.setter
    def headers(self, value: Dict[str, Any]) -> None:
        self._headers = value


class SyncHttpClient(HttpClient):
    """Synchronous HTTP Client"""

    def request(
        self,
        method: str,
        path: str,
        query: str = "",
        data: Any = None,
        request_id: str = "",
        headers: Optional[Dict[str, Any]] = None,
        allow_redirects: bool = True,
        timeout: Optional[float] = None,
    ) -> Tuple[int, bytes, CaseInsensitiveDict, RequestsCookieJar]:
        url = urlunparse((self._scheme, ":".join((self.host, str(self.port))), path, "", query, ""))
        request_id = f" [{request_id}]" if request_id else ""

        logger.debug("--->%s HTTP %s %s %s", request_id, method, url, data if data is not None else "")
        headers = headers if headers is not None else self.headers
        response = requests.request(
            method, url, data=data, headers=headers, allow_redirects=allow_redirects, timeout=timeout
        )

        code, data, ret_headers, cookies = response.status_code, response.content, response.headers, response.cookies
        logger.debug("<---%s HTTP %s %s code=%s data=%s", request_id, method, url, code, data)

        return code, data, ret_headers, cookies

    def get(
        self,
        path: str,
        query: str = "",
        request_id: str = "",
        headers: Optional[Dict[str, Any]] = None,
        allow_redirects: bool = True,
        timeout: Optional[float] = None,
    ) -> Tuple[int, bytes, CaseInsensitiveDict, RequestsCookieJar]:
        return self.request(
            "GET",
            path,
            query=query,
            request_id=request_id,
            headers=headers,
            allow_redirects=allow_redirects,
            timeout=timeout,
        )

    def post(
        self,
        path: str,
        query: str = "",
        data: Any = None,
        request_id: str = "",
        headers: Optional[Dict[str, Any]] = None,
        allow_redirects: bool = True,
        timeout: Optional[float] = None,
    ) -> Tuple[int, bytes, CaseInsensitiveDict, RequestsCookieJar]:
        return self.request(
            "POST",
            path,
            query=query,
            data=data,
            request_id=request_id,
            headers=headers,
            allow_redirects=allow_redirects,
            timeout=timeout,
        )

    def patch(
        self,
        path: str,
        query: str = "",
        data: Any = None,
        request_id: str = "",
        headers: Optional[Dict[str, Any]] = None,
        allow_redirects: bool = True,
        timeout: Optional[float] = None,
    ) -> Tuple[int, bytes, CaseInsensitiveDict, RequestsCookieJar]:
        return self.request(
            "PATCH",
            path,
            query=query,
            data=data,
            request_id=request_id,
            headers=headers,
            allow_redirects=allow_redirects,
            timeout=timeout,
        )

    def put(
        self,
        path: str,
        query: str = "",
        data: Any = None,
        request_id: str = "",
        headers: Optional[Dict[str, Any]] = None,
        allow_redirects: bool = True,
        timeout: Optional[float] = None,
    ) -> Tuple[int, bytes, CaseInsensitiveDict, RequestsCookieJar]:
        return self.request(
            "PUT",
            path,
            query=query,
            data=data,
            request_id=request_id,
            headers=headers,
            allow_redirects=allow_redirects,
            timeout=timeout,
        )

    def delete(
        self,
        path: str,
        query: str = "",
        data: Any = None,
        request_id: str = "",
        headers: Optional[Dict[str, Any]] = None,
        allow_redirects: bool = True,
        timeout: Optional[float] = None,
    ) -> Tuple[int, bytes, CaseInsensitiveDict, RequestsCookieJar]:
        return self.request(
            "DELETE",
            path,
            query=query,
            data=data,
            request_id=request_id,
            headers=headers,
            allow_redirects=allow_redirects,
            timeout=timeout,
        )


class AsyncHttpClient(HttpClient):
    """Asynchronous HTTP Client"""

    async def request(
        self,
        method: str,
        path: str,
        query: str = "",
        data: Any = None,
        request_id: str = "",
        headers: Optional[Dict[str, Any]] = None,
        allow_redirects: bool = True,
        timeout: Optional[float] = None,
    ) -> Tuple[int, bytes]:
        url = urlunparse((self._scheme, ":".join((self.host, str(self.port))), path, "", query, ""))
        request_id = f" [{request_id}]" if request_id else ""
        headers = headers if headers is not None else self.headers

        logger.debug("--->%s HTTP %s %s %s", request_id, method, url, data if data is not None else "")
        connector = aiohttp.TCPConnector(ssl=False)

        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.request(
                method, url, data=data, headers=headers, allow_redirects=allow_redirects, timeout=timeout
            ) as response:
                code, data = response.status, await response.read()
                logger.debug("<---%s HTTP %s %s code=%s data=%s", request_id, method, url, code, data)

                return code, data

    async def get(
        self,
        path: str,
        query: str = "",
        request_id: str = "",
        headers: Optional[Dict[str, Any]] = None,
        allow_redirects: bool = True,
        timeout: Optional[float] = None,
    ) -> Tuple[int, bytes]:
        return await self.request(
            "GET",
            path,
            query=query,
            request_id=request_id,
            headers=headers,
            allow_redirects=allow_redirects,
            timeout=timeout,
        )

    async def post(
        self,
        path: str,
        query: str = "",
        data: Any = None,
        request_id: str = "",
        headers: Optional[Dict[str, Any]] = None,
        allow_redirects: bool = True,
        timeout: Optional[float] = None,
    ) -> Tuple[int, bytes]:
        return await self.request(
            "POST",
            path,
            query=query,
            data=data,
            request_id=request_id,
            headers=headers,
            allow_redirects=allow_redirects,
            timeout=timeout,
        )

    async def patch(
        self,
        path: str,
        query: str = "",
        data: Any = None,
        request_id: str = "",
        headers: Optional[Dict[str, Any]] = None,
        allow_redirects: bool = True,
        timeout: Optional[float] = None,
    ) -> Tuple[int, bytes]:
        return await self.request(
            "PATCH",
            path,
            query=query,
            data=data,
            request_id=request_id,
            headers=headers,
            allow_redirects=allow_redirects,
            timeout=timeout,
        )

    async def put(
        self,
        path: str,
        query: str = "",
        data: Any = None,
        request_id: str = "",
        headers: Optional[Dict[str, Any]] = None,
        allow_redirects: bool = True,
        timeout: Optional[float] = None,
    ) -> Tuple[int, bytes]:
        return await self.request(
            "PUT",
            path,
            query=query,
            data=data,
            request_id=request_id,
            headers=headers,
            allow_redirects=allow_redirects,
            timeout=timeout,
        )

    async def delete(
        self,
        path: str,
        query: str = "",
        data: Any = None,
        request_id: str = "",
        headers: Optional[Dict[str, Any]] = None,
        allow_redirects: bool = True,
        timeout: Optional[float] = None,
    ) -> Tuple[int, bytes]:
        return await self.request(
            "DELETE",
            path,
            query=query,
            data=data,
            request_id=request_id,
            headers=headers,
            allow_redirects=allow_redirects,
            timeout=timeout,
        )

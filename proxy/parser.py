from typing import Tuple, List, Optional


class QueryParser:
    _HTTP_METHODS = ['GET', 'HEAD', 'OPTIONS', 'PUT', 'PATCH', 'POST', 'DELETE', 'TRACE', 'CONNECT']
    def __init__(self):
        self._direction = '==>'

    @staticmethod
    def _parse_request_line(request_l: str) -> dict:
        request_l = request_l.split()
        return dict(
            method=request_l[0],
            url_path=request_l[1],
            protocol=request_l[2],
        )

    @staticmethod
    def _parse_status_line(start_l: str) -> dict:
        start_l = start_l.split()
        return dict(
            protocol=start_l[0],
            status_code=start_l[1],
            status_text=start_l[2],
        )

    def _parse_start_line(self, start_l: str) -> dict:
        if start_l.split()[0] in self._HTTP_METHODS:
            self._direction = '<=='
            return self._parse_request_line(start_l)
        self._direction = '==>'
        return self._parse_status_line(start_l)

    @staticmethod
    def _parse_headers(headers_list: List[str]) -> dict:
        headers_dict = {}
        for header in headers_list:
            key, value = header.split(': ', 1)
            headers_dict[key] = value
        return headers_dict

    @staticmethod
    def _aggregate_head(start_line: dict, headers: dict) -> str:
        str_head = ' '.join(start_line.values()) + '\r\n'
        str_head += '\r\n'.join([f'{k}: {v}' for k, v in headers.items()]) + '\r\n\r\n'
        return str_head

    def parse_query(self, data: bytes) -> Tuple[bytes, bytes, Optional[int], str]:
        head, body = data.split(b'\r\n\r\n', 1)
        start_line, *headers = head.decode('utf-8').split('\r\n')
        start_line_dict = self._parse_start_line(start_line)
        headers_dict = self._parse_headers(headers)
        result = self._aggregate_head(start_line_dict, headers_dict)
        log_message = self._direction + " " + result.replace('\r\n', ' ')
        for k in ('content-length', 'Content-Length'):
            content_length = int(headers_dict.get(k, 0))
            if content_length > 0:
                break
        return result.encode('utf-8'), body, content_length, log_message

# coding: utf-8
from get_elevation import get_elevations

MAX_INPUT_SIZE = 250000
MAX_INPUT_POINTS = 10000
MAX_TILES_LOADS = 10

STATUS_NOT_FOUND = '404 Not Found'
STATUS_TOO_LARGE = '413 Request Entity Too Large'
STATUS_BAD_REQUEST = '400 Bad Request'


class HttpError(Exception):
    def __init__(self, status, message):
        self.status = status
        self.message = message


def read_points(fd, content_length):
    try:
        content_length = int(content_length)
    except ValueError:
        content_length = None
    if content_length is not None:
        if content_length > MAX_INPUT_SIZE:
            raise HttpError(STATUS_TOO_LARGE, 'Request too large')
        s = fd.read(content_length)
    else:
        s = fd.read(MAX_INPUT_SIZE + 1)
        if len(s) > MAX_INPUT_SIZE:
            raise HttpError(STATUS_TOO_LARGE, 'Request too large')
    points = []
    for line in s.splitlines():
        if not line:
            continue
        point = line.split()
        if len(point) != 2:
            raise HttpError(STATUS_BAD_REQUEST, 'Invalid request format')
        try:
            x = float(point[0])
            y = float(point[1])
        except ValueError:
            raise HttpError(STATUS_BAD_REQUEST, 'Invalid request format')
        points.append((x, y))
    return points


def process_request(environ, start_response):
    print environ
    if environ['PATH_INFO'] != '/' or environ['REQUEST_METHOD'] != 'POST':
        raise HttpError(STATUS_NOT_FOUND, 'Not found')
    points = read_points(environ['wsgi.input'], environ['CONTENT_LENGTH'])
    # TODO: limit number of loaded dem tiles
    elevations = get_elevations(points, environ['ELEVATIONS_DB_PATH'])
    status = '200 OK'
    response_headers = [('Access-Control-Allow-Origin', '*')]
    start_response(status, response_headers)
    elevations = [str(e) if e is not None else 'NULL' for e in elevations]
    return ['\n'.join(map(str, elevations))]


def application(environ, start_response):
    try:
        return process_request(environ, start_response)
    except HttpError as e:
        start_response(e.status, [])
        return [e.message]
        # import pprint
        # pprint.pprint(environ)


if __name__ == '__main__':
    import os

    os.environ['ELEVATIONS_DB_PATH'] = '/home/w/tmp/dem_tiles'
    from wsgiref.simple_server import make_server


    httpd = make_server('localhost', 8051, application)
    httpd.serve_forever()

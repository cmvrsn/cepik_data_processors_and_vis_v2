import json
import logging

from router import route_request

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def _response(status_code: int, body) -> dict:
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type,Authorization",
        },
        "body": json.dumps(body, ensure_ascii=False),
    }


def lambda_handler(event, context):
    try:
        status_code, payload = route_request(event)
        return _response(status_code, payload)
    except Exception as exc:
        logger.exception("Unhandled error")
        return _response(500, {"message": str(exc)})
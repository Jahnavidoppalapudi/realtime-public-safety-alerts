import pytest

from src.feeds import ConditionalFeed, build_session


class Response:
    def __init__(self, status, body=None, headers=None):
        self.status_code, self._body, self.headers = status, body, headers or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(self.status_code)

    def json(self):
        return self._body


class Session:
    def __init__(self, responses):
        self.responses, self.sent_headers = list(responses), []

    def get(self, url, headers, timeout):
        self.sent_headers.append(headers)
        return self.responses.pop(0)


def test_second_poll_is_conditional_and_304_means_nothing_new():
    session = Session([
        Response(200, {"features": [{"id": 1}]}, {"ETag": '"v1"', "Last-Modified": "Tue, 12 May"}),
        Response(304),
    ])  # fmt: skip
    feed = ConditionalFeed("usgs", "https://x")

    assert feed.fetch(session) == [{"id": 1}]
    assert feed.fetch(session) is None
    assert session.sent_headers[1] == {"If-None-Match": '"v1"', "If-Modified-Since": "Tue, 12 May"}
    assert feed.not_modified_count == 1


def test_response_without_features_is_an_error():
    with pytest.raises(ValueError, match="no 'features'"):
        ConditionalFeed("nws", "https://x").fetch(Session([Response(200, {"error": "?"})]))


@pytest.mark.parametrize("agent", ["", "my-app", "app (me@example.com)"])
def test_user_agent_needs_real_contact(agent):
    with pytest.raises(ValueError, match="CONTACT_USER_AGENT"):
        build_session(agent)

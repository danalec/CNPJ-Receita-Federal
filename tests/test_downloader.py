from src.downloader import get_session


def test_session_has_user_agent_header():
    s = get_session()
    ua = s.headers.get("User-Agent")
    assert isinstance(ua, str)
    assert len(ua) > 0

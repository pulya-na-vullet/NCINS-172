#!/usr/bin/env python3
"""Локальные проверки парсинга ответа Confluence search (без сети)."""

from export_user_pages import _page_from_search_result, _user_identity, _cql_user_value, safe_filename


def test_page_from_nested_content():
    result = {
        "title": "Outer",
        "content": {
            "id": "42",
            "type": "page",
            "title": "Inner Title",
            "space": {"key": "DEV"},
            "history": {"createdBy": {"displayName": "Ivan", "username": "ivan"}},
            "version": {"by": {"displayName": "Ivan"}},
            "_links": {"webui": "/pages/viewpage.action?pageId=42"},
        },
    }
    page = _page_from_search_result(result)
    assert page is not None
    assert page["id"] == "42"
    assert page["title"] == "Inner Title"
    assert page["space"] == "DEV"
    assert page["author"] == "Ivan"
    assert page["creatorUsername"] == "ivan"


def test_page_skips_non_page():
    result = {"content": {"id": "1", "type": "blogpost", "title": "x"}}
    assert _page_from_search_result(result) is None


def test_identity_and_cql_value():
    ident = _user_identity({"username": "YZab", "displayName": "Z", "userKey": "k1"})
    assert _cql_user_value(ident) == "YZab"
    ident2 = _user_identity({"accountId": "acc", "displayName": "Z"})
    assert _cql_user_value(ident2) == "acc"


def test_safe_filename():
    assert "page_9" in safe_filename("???", "9")
    assert "Hello World" in safe_filename("Hello World!", "1")


if __name__ == "__main__":
    test_page_from_nested_content()
    test_page_skips_non_page()
    test_identity_and_cql_value()
    test_safe_filename()
    print("OK")

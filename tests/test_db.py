import uuid

import pytest
from conftest import TEST_DATABASE_URL


def test_grafana_role_can_read_but_not_write():
    if not TEST_DATABASE_URL:
        pytest.skip("TEST_DATABASE_URL not set")
    psycopg = pytest.importorskip("psycopg")
    from psycopg.conninfo import conninfo_to_dict, make_conninfo

    from src.db import apply_schema

    name = f"grafana_{uuid.uuid4().hex[:8]}"
    role = f"reader_{uuid.uuid4().hex[:8]}"  # never touch the real grafana_reader login
    admin = psycopg.connect(TEST_DATABASE_URL, autocommit=True)
    admin.execute(f"create database {name}")
    try:
        params = conninfo_to_dict(TEST_DATABASE_URL)
        owner = psycopg.connect(make_conninfo(**{**params, "dbname": name}), autocommit=True)
        apply_schema(owner, grafana_password="s3cret-for-tests", reader_role=role)
        apply_schema(owner, grafana_password="s3cret-for-tests", reader_role=role)  # re-run
        owner.execute("create table added_later (x int)")  # covered by default privileges
        owner.close()

        reader_params = {**params, "dbname": name, "user": role,
                         "password": "s3cret-for-tests"}  # fmt: skip
        with psycopg.connect(make_conninfo(**reader_params), autocommit=True) as reader:
            assert reader.execute("select count(*) from v_active_incidents").fetchone() == (0,)
            assert reader.execute("select count(*) from added_later").fetchone() == (0,)
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                reader.execute("delete from incidents")
    finally:
        admin.execute(f"drop database {name} with (force)")
        admin.execute(f"drop role if exists {role}")
        admin.close()

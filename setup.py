import sqlalchemy
from ORM.postgres import ORM

orm = ORM()
conn = orm.get_connection()

metadata = sqlalchemy.MetaData()

if not orm.engine.dialect.has_schema(conn, "raw"):
    conn.execute(sqlalchemy.schema.CreateSchema("raw"))

if not orm.engine.dialect.has_schema(conn, "cleansed"):
    conn.execute(sqlalchemy.schema.CreateSchema("cleansed"))

longraw = sqlalchemy.Table("long", metadata,
                    sqlalchemy.Column("parameter", sqlalchemy.String(64)),
                    sqlalchemy.Column("lat", sqlalchemy.String(32)),
                    sqlalchemy.Column("lon", sqlalchemy.String(32)),
                    sqlalchemy.Column("date", sqlalchemy.DateTime()),
                    sqlalchemy.Column("value", sqlalchemy.String(128)),
                    keep_existing=True, schema="raw"
                    )

longcleansed = sqlalchemy.Table("long", metadata,
                    sqlalchemy.Column("parameter", sqlalchemy.String(64)),
                    sqlalchemy.Column("lat", sqlalchemy.String(32)),
                    sqlalchemy.Column("lon", sqlalchemy.String(32)),
                    sqlalchemy.Column("date", sqlalchemy.DateTime()),
                    sqlalchemy.Column("value", sqlalchemy.String(128)),
                    keep_existing=True, schema="cleansed"
                    )

wideraw = sqlalchemy.Table("wide", metadata,
                    sqlalchemy.Column("wind_speed_10m:ms", sqlalchemy.DECIMAL(11,2)),
                    sqlalchemy.Column("wind_dir_10m:d", sqlalchemy.DECIMAL(11,2)),
                    sqlalchemy.Column("wind_gusts_10m_1h:ms", sqlalchemy.DECIMAL(11,2)),
                    sqlalchemy.Column("wind_gusts_10m_24h:ms", sqlalchemy.DECIMAL(11,2)),
                    sqlalchemy.Column("lat", sqlalchemy.String(32)),
                    sqlalchemy.Column("lon", sqlalchemy.String(32)),
                    sqlalchemy.Column("date", sqlalchemy.DateTime()),
                    keep_existing=True, schema="raw"
                    )

widecleansed = sqlalchemy.Table("wide", metadata,
                    sqlalchemy.Column("wind_speed_10m:ms", sqlalchemy.DECIMAL(11,2)),
                    sqlalchemy.Column("wind_dir_10m:d", sqlalchemy.DECIMAL(11,2)),
                    sqlalchemy.Column("wind_gusts_10m_1h:ms", sqlalchemy.DECIMAL(11,2)),
                    sqlalchemy.Column("wind_gusts_10m_24h:ms", sqlalchemy.DECIMAL(11,2)),
                    sqlalchemy.Column("lat", sqlalchemy.String(32)),
                    sqlalchemy.Column("lon", sqlalchemy.String(32)),
                    sqlalchemy.Column("date", sqlalchemy.DateTime()),
                    keep_existing=True, schema="cleansed"
                    )


metadata.create_all(conn, checkfirst=True)
conn.commit()

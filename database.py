from sqlalchemy import create_engine, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from urllib.parse import quote_plus
"""ssh -L 3307:localhost:3306 azuser1@51.105.6.216"""


SQLALCHEMY_DATABASE_URL = 'mysql+mysqlconnector://sainsbury_dashboard_db_user:ghjkhfajds@centraldb.mysql.database.azure.com:3306/sainsburys_db'
XML_DATABASE_URL = "mysql+mysqlconnector://userdb:pass@central.mysql.database.azure.com:3306/xmldata_sains_sco"
INTERNAL_DEV_XML_DATABASE_URL = "mysql+mysqlconnector://xmldata_sains_sco_dashboard_:Owlys77@internal-dev.6e5l3lii71.eu-west-2.rds.amazonaws.com:3306/xmldata_sains_sco"

engine = create_engine(SQLALCHEMY_DATABASE_URL, pool_size=20, max_overflow=40)
engine_xml = create_engine(XML_DATABASE_URL, pool_size=20, max_overflow=40)
engine_internal_dev_xml = create_engine(INTERNAL_DEV_XML_DATABASE_URL, pool_size=10, max_overflow=20)
print(engine)
print(engine.connect())
print(engine_xml)
print(engine_xml.connect())
print(engine_internal_dev_xml)
print(engine_internal_dev_xml.connect())

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
SessionLocalXML = sessionmaker(autocommit=False, autoflush=False, bind=engine_xml)
SessionLocalInternalDevXML = sessionmaker(autocommit=False, autoflush=False, bind=engine_internal_dev_xml)

Base = declarative_base()
BaseXML = declarative_base(engine_xml)
BaseInternalDevXML = declarative_base(engine_internal_dev_xml)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_xml_db():
    db = SessionLocalXML()
    try:
        yield db
    finally:
        db.close()


def get_internal_dev_xml_db():
    db = SessionLocalInternalDevXML()
    try:
        yield db
    finally:
        db.close()


from database import Base, BaseXML, BaseInternalDevXML, engine, engine_xml, engine_internal_dev_xml
from sqlalchemy import Table


class Transactions(Base):
    __table__ = Table('transactions', Base.metadata, autoload=True, autoload_with=engine)


class Stores(Base):
    __table__ = Table('stores', Base.metadata, autoload=True, autoload_with=engine)


class Transaction_items(Base):
    __table__ = Table('transaction_items', Base.metadata, autoload=True, autoload_with=engine)


# class Outage(Base):
#     __table__ = Table('outage', Base.metadata, autoload=True, autoload_with=engine)


class Sources(Base):
    __table__ = Table('sources', Base.metadata, autoload=True, autoload_with=engine)


class Operators(Base):
    __table__ = Table('operators', Base.metadata, autoload=True, autoload_with=engine)


class Comments(Base):
    __table__ = Table('comments', Base.metadata, autoload=True, autoload_with=engine)


# class AppUsers(Base):
#     __table__ = Table('app_user', Base.metadata, autoload=True, autoload_with=engine)


# class IssueCategory(Base):
#     __table__ = Table("issue_category", Base.metadata, autoload=True, autoload_with=engine)


# class IssueDescription(Base):
#     __table__ = Table("issue_description", Base.metadata, autoload=True, autoload_with=engine)


class Users(Base):
    __table__ = Table("users", Base.metadata, autoload=True, autoload_with=engine)


class Tracker(Base):
    __table__ = Table("tracker", Base.metadata, autoload=True, autoload_with=engine)


class DailyProcessedData(Base):
    __table__ = Table("daily_processed_date", Base.metadata, autoload=True, autoload_with=engine)


class UserView(Base):
    __table__ = Table("user_view", Base.metadata, autoload=True, autoload_with=engine)


class CameraInfo(Base):
    __table__ = Table("camera_info", Base.metadata, autoload=True, autoload_with=engine)


class UpdatedInfo(Base):
    __table__ = Table("updated_info", Base.metadata, autoload=True, autoload_with=engine)


class AisleImages(Base):
    __table__ = Table("aisle_images", Base.metadata, autoload=True, autoload_with=engine)


class Status(Base):
    __table__ = Table("status", Base.metadata, autoload=True, autoload_with=engine)


class LatestStatus(Base):
    __table__ = Table("latest_status", Base.metadata, autoload=True, autoload_with=engine)


class TransactionsLossNoLossData(Base):
    __table__ = Table("transactions_loss_noloss_data", Base.metadata, autoload=True, autoload_with=engine)


class StoreCameraMapping(Base):
    __table__ = Table("store_camera_sys_mapping", Base.metadata, autoload=True, autoload_with=engine)


class StoreHours(Base):
    __table__ = Table('store_hours', Base.metadata, autoload=True, autoload_with=engine)


class Blacklist_JWT(Base):
    __table__ = Table("blacklisted_jti", Base.metadata, autoload=True, autoload_with=engine)


class ApplicationStoreStatus(Base):
    __table__ = Table("application_store_status", Base.metadata, autoload=True, autoload_with=engine)


class StreamData(Base):
    __table__ = Table("stream_data", Base.metadata, autoload=True, autoload_with=engine)


class VTCData(Base):
    __table__ = Table("vtc_data", Base.metadata, autoload=True, autoload_with=engine)


class JitterData(Base):
    __table__ = Table("jitter_data", Base.metadata, autoload=True, autoload_with=engine)


class TransactionMain(BaseXML):
    __table__ = Table("transactions", BaseXML.metadata, autoload=True, autoload_with=engine_xml)


class TransactionSCO(BaseXML):
    __table__ = Table("transactions_sco", BaseXML.metadata, autoload=True, autoload_with=engine_xml)


class TransactionDetailsSco(BaseXML):
    __table__ = Table('transaction_details_sco', BaseXML.metadata, autoload=True, autoload_with=engine_xml)


class OverallReportAggregatedResult(BaseXML):
    __table__ = Table('overall_report_aggregated_result', BaseXML.metadata, autoload=True, autoload_with=engine_xml)


class Transaction_Main(BaseXML):
    __table__ = Table("transactions", BaseXML.metadata, autoload=True, autoload_with=engine_xml)


class Transaction_Details_Main(BaseXML):
    __table__ = Table("transaction_details", BaseXML.metadata, autoload=True, autoload_with=engine_xml)


class StoresScoAutomationConfig(BaseXML):
    __table__ = Table("sco_automation_config_data", Base.metadata, autoload=True, autoload_with=engine)


class ScoConfigChangeLogs(BaseXML):
    __table__ = Table("sco_config_data_change_log", Base.metadata, autoload=True, autoload_with=engine)


class StoresInternalDev(BaseInternalDevXML):
    __table__ = Table("stores", BaseInternalDevXML.metadata, autoload=True, autoload_with=engine_internal_dev_xml)


class TransactionDetailsSCOInternalDev(BaseInternalDevXML):
    __table__ = Table("transaction_details_sco", BaseInternalDevXML.metadata, autoload=True, autoload_with=engine_internal_dev_xml)


class TransactionSCOAlertInternalDev(BaseInternalDevXML):
    __table__ = Table("transactions_sco_alert", BaseInternalDevXML.metadata, autoload=True, autoload_with=engine_internal_dev_xml)

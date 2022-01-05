CREATE TABLE PS_D_ENRL_DTL_STAT
(
  ENRL_DETL_STAT_SID  NUMBER(10),
  ENRL_REQ_DETL_STAT  VARCHAR2(4 CHAR),
  DTL_STATUS_SD       VARCHAR2(10 CHAR),
  DTL_STATUS_LD       VARCHAR2(30 CHAR),
  SRC_SYS_ID          VARCHAR2(5 CHAR),
  EFF_START_DT        DATE,
  EFF_END_DT          DATE,
  CURRENT_IND         VARCHAR2(1 CHAR),
  LOAD_ERROR          VARCHAR2(1 CHAR),
  DATA_ORIGIN         VARCHAR2(1 CHAR),
  CREATED_EW_DTTM     DATE,
  LASTUPD_EW_DTTM     DATE,
  BATCH_SID           NUMBER(10)
)
NOLOGGING 
NOCOMPRESS 
NO INMEMORY
NOCACHE
RESULT_CACHE (MODE DEFAULT)
NOPARALLEL;

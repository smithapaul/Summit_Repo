DROP TABLE CSSTG_OWNER.PSROLEDEFN CASCADE CONSTRAINTS
/

--
-- PSROLEDEFN  (Table) 
--
CREATE TABLE CSSTG_OWNER.PSROLEDEFN
(
  ROLENAME             VARCHAR2(30 BYTE),
  VERSION              INTEGER,
  ROLETYPE             VARCHAR2(1 BYTE),
  DESCR                VARCHAR2(30 BYTE),
  QRYNAME              VARCHAR2(30 BYTE),
  ROLESTATUS           VARCHAR2(1 BYTE),
  RECNAME              VARCHAR2(15 BYTE),
  FIELDNAME            VARCHAR2(18 BYTE),
  PC_EVENT_TYPE        VARCHAR2(14 BYTE),
  QRYNAME_SEC          VARCHAR2(30 BYTE),
  PC_FUNCTION_NAME     VARCHAR2(30 BYTE),
  ROLE_PCODE_RULE_ON   VARCHAR2(1 BYTE),
  ROLE_QUERY_RULE_ON   VARCHAR2(1 BYTE),
  LDAP_RULE_ON         VARCHAR2(1 BYTE),
  ALLOWNOTIFY          VARCHAR2(1 BYTE),
  ALLOWLOOKUP          VARCHAR2(1 BYTE),
  LASTUPDDTTM          TIMESTAMP(6),
  LASTUPDOPRID         VARCHAR2(30 BYTE),
  PARTITION_INDICATOR  INTEGER                  NOT NULL,
  LOADTIME             DATE,
  DESCRLONG            CLOB
)
LOB (DESCRLONG) STORE AS BASICFILE (
  TABLESPACE  CSSTG_DATA1
  ENABLE      STORAGE IN ROW
  CHUNK       16384
  PCTVERSION  10)
COMPRESS BASIC
PARTITION BY LIST (PARTITION_INDICATOR)
(  
  PARTITION PARTITION_0 VALUES (0)
    COMPRESS BASIC
    LOB (DESCRLONG) STORE AS BASICFILE (
      TABLESPACE  CSSTG_DATA1
      ENABLE      STORAGE IN ROW
      CHUNK       16384
      RETENTION
      STORAGE    (
                  INITIAL          80K
                  NEXT             1M
                  MINEXTENTS       1
                  MAXEXTENTS       UNLIMITED
                  PCTINCREASE      0
                  BUFFER_POOL      DEFAULT
                 )),  
  PARTITION PARTITION_1 VALUES (1)
    COMPRESS BASIC
    LOB (DESCRLONG) STORE AS BASICFILE (
      TABLESPACE  CSSTG_DATA1
      ENABLE      STORAGE IN ROW
      CHUNK       16384
      RETENTION
      STORAGE    (
                  INITIAL          80K
                  NEXT             1M
                  MINEXTENTS       1
                  MAXEXTENTS       UNLIMITED
                  PCTINCREASE      0
                  BUFFER_POOL      DEFAULT
                 ))
)
/

COMMENT ON TABLE CSSTG_OWNER.PSROLEDEFN IS '
Stage table containg data from table of the same name in PeopleSoft.
This copy of the table employs partition swapping.  It has two partitions each having a complete set of the data.
Each time the load process runs the alternate partition is loaded and it is made current in table PARTITION_SWAP_CONTROL.
When PSROLEDEFN is used it is jopined with PARTITION_SWAP_CONTROL so that only the current partition is used.
This partition swapping ensures that the table is always available to other marts.'
/

COMMENT ON COLUMN CSSTG_OWNER.PSROLEDEFN.PARTITION_INDICATOR IS 'The value of this column determines the partition of the data.  0 for PARTITION_0, 1 for PARTITION_1'
/

COMMENT ON COLUMN CSSTG_OWNER.PSROLEDEFN.LOADTIME IS 'Time that the row was loaded'
/

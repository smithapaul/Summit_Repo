CREATE TABLE UM_M_AF_STDNT_ADM_AMH_DAILY
(
  RUN_DT             DATE,
  INSTITUTION_CD     VARCHAR2(5 BYTE),
  ACAD_CAR_CD        VARCHAR2(5 BYTE),
  ACAD_PROG_CD       VARCHAR2(8 BYTE),
  ACAD_PLAN_CD       VARCHAR2(10 BYTE),
  ADMIT_TERM_CD      VARCHAR2(4 BYTE),
  PERSON_ID          VARCHAR2(11 BYTE),
  ADM_APPL_NBR       VARCHAR2(8 BYTE),
  SLATE_ID           VARCHAR2(100 BYTE),
  EXT_ADM_APPL_NBR   VARCHAR2(30 BYTE),
  SRC_SYS_ID         VARCHAR2(5 BYTE),
  INSTITUTION_LD     VARCHAR2(50 BYTE),
  ACAD_CAR_LD        VARCHAR2(50 BYTE),
  ACAD_PROG_LD       VARCHAR2(50 BYTE),
  ACAD_PLAN_LD       VARCHAR2(50 BYTE),
  ADMIT_TERM_LD      VARCHAR2(50 BYTE),
  REPORTING_TERM_CD  VARCHAR2(4 BYTE),
  REPORTING_TERM_LD  VARCHAR2(50 BYTE),
  ACAD_YR            VARCHAR2(4 BYTE),
  FISCAL_YR          VARCHAR2(4 BYTE),
  ACAD_ORG_CD        VARCHAR2(5 BYTE),
  ACAD_ORG_LD        VARCHAR2(50 BYTE),
  ADMIT_TYPE_ID      VARCHAR2(5 BYTE),
  ADMIT_TYPE_LD      VARCHAR2(50 BYTE),
  ADMIT_TYPE_GRP     VARCHAR2(50 BYTE),
  APPL_CNTR_ID       VARCHAR2(8 BYTE),
  CE_APPL_FLG        VARCHAR2(1 BYTE),
  EDU_LVL_CD         VARCHAR2(10 BYTE),
  EDU_LVL_LD         VARCHAR2(50 BYTE),
  IS_RSDNCY_FLG      VARCHAR2(1 BYTE),
  PLAN_CIP_CD        VARCHAR2(13 BYTE),
  PLAN_CIP_LD        VARCHAR2(50 BYTE),
  RSDNCY_ID          VARCHAR2(5 BYTE),
  RSDNCY_LD          VARCHAR2(50 BYTE),
  APPL_CNT           NUMBER,
  ADMIT_CNT          NUMBER,
  DENY_CNT           NUMBER,
  DEPOSIT_CNT        NUMBER,
  ENROLL_CNT         NUMBER,
  ENROLL_SUBSEQ_CNT  NUMBER,
  UNDUP_CNT          NUMBER,
  CREATED_EW_DTTM    DATE
)
COMPRESS BASIC
NO INMEMORY
PARTITION BY LIST (RUN_DT) AUTOMATIC
(  
  PARTITION DATE_2021_09_10 VALUES (TO_DATE(' 2021-09-10 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_09_11 VALUES (TO_DATE(' 2021-09-11 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_09_14 VALUES (TO_DATE(' 2021-09-14 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_09_15 VALUES (TO_DATE(' 2021-09-15 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_09_09 VALUES (TO_DATE(' 2021-09-09 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_09_02 VALUES (TO_DATE(' 2021-09-02 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_08_24 VALUES (TO_DATE(' 2021-08-24 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_08_25 VALUES (TO_DATE(' 2021-08-25 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_08_26 VALUES (TO_DATE(' 2021-08-26 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_08_27 VALUES (TO_DATE(' 2021-08-27 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_08_28 VALUES (TO_DATE(' 2021-08-28 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_08_31 VALUES (TO_DATE(' 2021-08-31 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_09_03 VALUES (TO_DATE(' 2021-09-03 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_09_04 VALUES (TO_DATE(' 2021-09-04 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_09_07 VALUES (TO_DATE(' 2021-09-07 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_09_08 VALUES (TO_DATE(' 2021-09-08 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_09_16 VALUES (TO_DATE(' 2021-09-16 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_09_17 VALUES (TO_DATE(' 2021-09-17 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_09_18 VALUES (TO_DATE(' 2021-09-18 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_09_20 VALUES (TO_DATE(' 2021-09-20 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_09_21 VALUES (TO_DATE(' 2021-09-21 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_09_22 VALUES (TO_DATE(' 2021-09-22 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_09_23 VALUES (TO_DATE(' 2021-09-23 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_09_24 VALUES (TO_DATE(' 2021-09-24 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_09_25 VALUES (TO_DATE(' 2021-09-25 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_09_28 VALUES (TO_DATE(' 2021-09-28 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_09_29 VALUES (TO_DATE(' 2021-09-29 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_09_30 VALUES (TO_DATE(' 2021-09-30 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_10_01 VALUES (TO_DATE(' 2021-10-01 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_10_02 VALUES (TO_DATE(' 2021-10-02 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_10_04 VALUES (TO_DATE(' 2021-10-04 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_10_05 VALUES (TO_DATE(' 2021-10-05 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_10_06 VALUES (TO_DATE(' 2021-10-06 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_10_07 VALUES (TO_DATE(' 2021-10-07 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_10_08 VALUES (TO_DATE(' 2021-10-08 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_10_12 VALUES (TO_DATE(' 2021-10-12 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_10_13 VALUES (TO_DATE(' 2021-10-13 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_10_14 VALUES (TO_DATE(' 2021-10-14 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_10_15 VALUES (TO_DATE(' 2021-10-15 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_10_16 VALUES (TO_DATE(' 2021-10-16 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_10_18 VALUES (TO_DATE(' 2021-10-18 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_10_19 VALUES (TO_DATE(' 2021-10-19 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_10_20 VALUES (TO_DATE(' 2021-10-20 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_10_21 VALUES (TO_DATE(' 2021-10-21 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_10_22 VALUES (TO_DATE(' 2021-10-22 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_10_23 VALUES (TO_DATE(' 2021-10-23 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_10_25 VALUES (TO_DATE(' 2021-10-25 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_10_26 VALUES (TO_DATE(' 2021-10-26 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_10_27 VALUES (TO_DATE(' 2021-10-27 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_10_28 VALUES (TO_DATE(' 2021-10-28 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_10_29 VALUES (TO_DATE(' 2021-10-29 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_10_30 VALUES (TO_DATE(' 2021-10-30 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_11_01 VALUES (TO_DATE(' 2021-11-01 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_11_02 VALUES (TO_DATE(' 2021-11-02 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_11_03 VALUES (TO_DATE(' 2021-11-03 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_11_04 VALUES (TO_DATE(' 2021-11-04 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_11_05 VALUES (TO_DATE(' 2021-11-05 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_11_06 VALUES (TO_DATE(' 2021-11-06 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_11_08 VALUES (TO_DATE(' 2021-11-08 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_11_09 VALUES (TO_DATE(' 2021-11-09 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_11_10 VALUES (TO_DATE(' 2021-11-10 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_11_11 VALUES (TO_DATE(' 2021-11-11 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_11_12 VALUES (TO_DATE(' 2021-11-12 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_11_13 VALUES (TO_DATE(' 2021-11-13 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_11_15 VALUES (TO_DATE(' 2021-11-15 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_11_16 VALUES (TO_DATE(' 2021-11-16 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_11_17 VALUES (TO_DATE(' 2021-11-17 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_11_18 VALUES (TO_DATE(' 2021-11-18 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_11_19 VALUES (TO_DATE(' 2021-11-19 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_11_20 VALUES (TO_DATE(' 2021-11-20 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_11_22 VALUES (TO_DATE(' 2021-11-22 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_11_23 VALUES (TO_DATE(' 2021-11-23 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_11_24 VALUES (TO_DATE(' 2021-11-24 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_11_25 VALUES (TO_DATE(' 2021-11-25 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_11_26 VALUES (TO_DATE(' 2021-11-26 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_11_27 VALUES (TO_DATE(' 2021-11-27 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_11_29 VALUES (TO_DATE(' 2021-11-29 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_11_30 VALUES (TO_DATE(' 2021-11-30 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_12_01 VALUES (TO_DATE(' 2021-12-01 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_12_02 VALUES (TO_DATE(' 2021-12-02 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_12_03 VALUES (TO_DATE(' 2021-12-03 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_12_04 VALUES (TO_DATE(' 2021-12-04 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_12_06 VALUES (TO_DATE(' 2021-12-06 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_12_07 VALUES (TO_DATE(' 2021-12-07 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_12_08 VALUES (TO_DATE(' 2021-12-08 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_12_09 VALUES (TO_DATE(' 2021-12-09 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_12_10 VALUES (TO_DATE(' 2021-12-10 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_12_11 VALUES (TO_DATE(' 2021-12-11 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_12_13 VALUES (TO_DATE(' 2021-12-13 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_12_15 VALUES (TO_DATE(' 2021-12-15 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_12_16 VALUES (TO_DATE(' 2021-12-16 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_12_17 VALUES (TO_DATE(' 2021-12-17 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_12_18 VALUES (TO_DATE(' 2021-12-18 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_12_20 VALUES (TO_DATE(' 2021-12-20 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_12_21 VALUES (TO_DATE(' 2021-12-21 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_12_22 VALUES (TO_DATE(' 2021-12-22 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_12_23 VALUES (TO_DATE(' 2021-12-23 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_12_24 VALUES (TO_DATE(' 2021-12-24 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_12_25 VALUES (TO_DATE(' 2021-12-25 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_12_27 VALUES (TO_DATE(' 2021-12-27 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_12_28 VALUES (TO_DATE(' 2021-12-28 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_12_29 VALUES (TO_DATE(' 2021-12-29 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_12_30 VALUES (TO_DATE(' 2021-12-30 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2021_12_31 VALUES (TO_DATE(' 2021-12-31 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2022_01_01 VALUES (TO_DATE(' 2022-01-01 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2022_01_03 VALUES (TO_DATE(' 2022-01-03 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2022_01_04 VALUES (TO_DATE(' 2022-01-04 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC,  
  PARTITION DATE_2022_01_05 VALUES (TO_DATE(' 2022-01-05 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    READ WRITE
    NO INMEMORY
    NOLOGGING
    COMPRESS BASIC
)
NOCACHE
RESULT_CACHE (MODE DEFAULT)
NOPARALLEL;

DROP TABLE CSMRT_OWNER.UM_M_AF_STDNT_ADM_AMH_DAILY CASCADE CONSTRAINTS
/

--
-- UM_M_AF_STDNT_ADM_AMH_DAILY  (Table) 
--
CREATE TABLE CSMRT_OWNER.UM_M_AF_STDNT_ADM_AMH_DAILY
(
  RUN_DT             DATE                       NOT NULL,
  INSTITUTION_CD     VARCHAR2(5 BYTE)           NOT NULL,
  ACAD_CAR_CD        VARCHAR2(5 BYTE)           NOT NULL,
  ACAD_PROG_CD       VARCHAR2(8 BYTE)           NOT NULL,
  ACAD_PLAN_CD       VARCHAR2(10 BYTE)          NOT NULL,
  ADMIT_TERM_CD      VARCHAR2(4 BYTE)           NOT NULL,
  PERSON_ID          VARCHAR2(11 BYTE)          NOT NULL,
  ADM_APPL_NBR       VARCHAR2(8 BYTE)           NOT NULL,
  SLATE_ID           VARCHAR2(100 BYTE)         NOT NULL,
  EXT_ADM_APPL_NBR   VARCHAR2(30 BYTE)          NOT NULL,
  SRC_SYS_ID         VARCHAR2(5 BYTE)           NOT NULL,
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
PARTITION BY LIST (RUN_DT) AUTOMATIC
(  
  PARTITION DATE_2021_09_10 VALUES (TO_DATE(' 2021-09-10 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_09_11 VALUES (TO_DATE(' 2021-09-11 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_09_14 VALUES (TO_DATE(' 2021-09-14 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_09_15 VALUES (TO_DATE(' 2021-09-15 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_09_09 VALUES (TO_DATE(' 2021-09-09 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_09_02 VALUES (TO_DATE(' 2021-09-02 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_08_24 VALUES (TO_DATE(' 2021-08-24 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_08_25 VALUES (TO_DATE(' 2021-08-25 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_08_26 VALUES (TO_DATE(' 2021-08-26 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_08_27 VALUES (TO_DATE(' 2021-08-27 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_08_28 VALUES (TO_DATE(' 2021-08-28 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_08_31 VALUES (TO_DATE(' 2021-08-31 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_09_03 VALUES (TO_DATE(' 2021-09-03 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_09_04 VALUES (TO_DATE(' 2021-09-04 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_09_07 VALUES (TO_DATE(' 2021-09-07 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_09_08 VALUES (TO_DATE(' 2021-09-08 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_09_16 VALUES (TO_DATE(' 2021-09-16 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_09_17 VALUES (TO_DATE(' 2021-09-17 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_09_18 VALUES (TO_DATE(' 2021-09-18 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_09_20 VALUES (TO_DATE(' 2021-09-20 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_09_21 VALUES (TO_DATE(' 2021-09-21 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_09_22 VALUES (TO_DATE(' 2021-09-22 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_09_23 VALUES (TO_DATE(' 2021-09-23 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_09_24 VALUES (TO_DATE(' 2021-09-24 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_09_25 VALUES (TO_DATE(' 2021-09-25 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_09_28 VALUES (TO_DATE(' 2021-09-28 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_09_29 VALUES (TO_DATE(' 2021-09-29 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_09_30 VALUES (TO_DATE(' 2021-09-30 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_10_01 VALUES (TO_DATE(' 2021-10-01 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_10_02 VALUES (TO_DATE(' 2021-10-02 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_10_04 VALUES (TO_DATE(' 2021-10-04 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_10_05 VALUES (TO_DATE(' 2021-10-05 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_10_06 VALUES (TO_DATE(' 2021-10-06 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_10_07 VALUES (TO_DATE(' 2021-10-07 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_10_08 VALUES (TO_DATE(' 2021-10-08 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_10_12 VALUES (TO_DATE(' 2021-10-12 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_10_13 VALUES (TO_DATE(' 2021-10-13 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_10_14 VALUES (TO_DATE(' 2021-10-14 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_10_15 VALUES (TO_DATE(' 2021-10-15 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_10_16 VALUES (TO_DATE(' 2021-10-16 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_10_18 VALUES (TO_DATE(' 2021-10-18 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_10_19 VALUES (TO_DATE(' 2021-10-19 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_10_20 VALUES (TO_DATE(' 2021-10-20 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_10_21 VALUES (TO_DATE(' 2021-10-21 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_10_22 VALUES (TO_DATE(' 2021-10-22 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_10_23 VALUES (TO_DATE(' 2021-10-23 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_10_25 VALUES (TO_DATE(' 2021-10-25 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_10_26 VALUES (TO_DATE(' 2021-10-26 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_10_27 VALUES (TO_DATE(' 2021-10-27 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_10_28 VALUES (TO_DATE(' 2021-10-28 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_10_29 VALUES (TO_DATE(' 2021-10-29 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_10_30 VALUES (TO_DATE(' 2021-10-30 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_11_01 VALUES (TO_DATE(' 2021-11-01 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_11_02 VALUES (TO_DATE(' 2021-11-02 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_11_03 VALUES (TO_DATE(' 2021-11-03 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_11_04 VALUES (TO_DATE(' 2021-11-04 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_11_05 VALUES (TO_DATE(' 2021-11-05 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_11_06 VALUES (TO_DATE(' 2021-11-06 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_11_08 VALUES (TO_DATE(' 2021-11-08 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_11_09 VALUES (TO_DATE(' 2021-11-09 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_11_10 VALUES (TO_DATE(' 2021-11-10 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_11_11 VALUES (TO_DATE(' 2021-11-11 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_11_12 VALUES (TO_DATE(' 2021-11-12 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_11_13 VALUES (TO_DATE(' 2021-11-13 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_11_15 VALUES (TO_DATE(' 2021-11-15 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_11_16 VALUES (TO_DATE(' 2021-11-16 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_11_17 VALUES (TO_DATE(' 2021-11-17 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_11_18 VALUES (TO_DATE(' 2021-11-18 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_11_19 VALUES (TO_DATE(' 2021-11-19 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_11_20 VALUES (TO_DATE(' 2021-11-20 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_11_22 VALUES (TO_DATE(' 2021-11-22 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_11_23 VALUES (TO_DATE(' 2021-11-23 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_11_24 VALUES (TO_DATE(' 2021-11-24 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_11_25 VALUES (TO_DATE(' 2021-11-25 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_11_26 VALUES (TO_DATE(' 2021-11-26 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_11_27 VALUES (TO_DATE(' 2021-11-27 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_11_29 VALUES (TO_DATE(' 2021-11-29 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_11_30 VALUES (TO_DATE(' 2021-11-30 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_12_01 VALUES (TO_DATE(' 2021-12-01 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_12_02 VALUES (TO_DATE(' 2021-12-02 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_12_03 VALUES (TO_DATE(' 2021-12-03 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_12_04 VALUES (TO_DATE(' 2021-12-04 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_12_06 VALUES (TO_DATE(' 2021-12-06 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_12_07 VALUES (TO_DATE(' 2021-12-07 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_12_08 VALUES (TO_DATE(' 2021-12-08 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_12_09 VALUES (TO_DATE(' 2021-12-09 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_12_10 VALUES (TO_DATE(' 2021-12-10 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_12_11 VALUES (TO_DATE(' 2021-12-11 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_12_13 VALUES (TO_DATE(' 2021-12-13 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_12_15 VALUES (TO_DATE(' 2021-12-15 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_12_16 VALUES (TO_DATE(' 2021-12-16 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_12_17 VALUES (TO_DATE(' 2021-12-17 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_12_18 VALUES (TO_DATE(' 2021-12-18 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_12_20 VALUES (TO_DATE(' 2021-12-20 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_12_21 VALUES (TO_DATE(' 2021-12-21 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_12_22 VALUES (TO_DATE(' 2021-12-22 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_12_23 VALUES (TO_DATE(' 2021-12-23 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_12_24 VALUES (TO_DATE(' 2021-12-24 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_12_25 VALUES (TO_DATE(' 2021-12-25 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_12_27 VALUES (TO_DATE(' 2021-12-27 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_12_28 VALUES (TO_DATE(' 2021-12-28 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_12_29 VALUES (TO_DATE(' 2021-12-29 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_12_30 VALUES (TO_DATE(' 2021-12-30 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2021_12_31 VALUES (TO_DATE(' 2021-12-31 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_01_01 VALUES (TO_DATE(' 2022-01-01 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_01_03 VALUES (TO_DATE(' 2022-01-03 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_01_04 VALUES (TO_DATE(' 2022-01-04 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_01_05 VALUES (TO_DATE(' 2022-01-05 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_01_06 VALUES (TO_DATE(' 2022-01-06 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_01_07 VALUES (TO_DATE(' 2022-01-07 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_01_08 VALUES (TO_DATE(' 2022-01-08 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_01_10 VALUES (TO_DATE(' 2022-01-10 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_01_11 VALUES (TO_DATE(' 2022-01-11 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_01_12 VALUES (TO_DATE(' 2022-01-12 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_01_13 VALUES (TO_DATE(' 2022-01-13 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_01_14 VALUES (TO_DATE(' 2022-01-14 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_01_15 VALUES (TO_DATE(' 2022-01-15 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_01_17 VALUES (TO_DATE(' 2022-01-17 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_01_18 VALUES (TO_DATE(' 2022-01-18 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_01_19 VALUES (TO_DATE(' 2022-01-19 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_01_20 VALUES (TO_DATE(' 2022-01-20 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_01_21 VALUES (TO_DATE(' 2022-01-21 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_01_22 VALUES (TO_DATE(' 2022-01-22 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_01_24 VALUES (TO_DATE(' 2022-01-24 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_01_25 VALUES (TO_DATE(' 2022-01-25 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_01_26 VALUES (TO_DATE(' 2022-01-26 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_01_27 VALUES (TO_DATE(' 2022-01-27 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_01_28 VALUES (TO_DATE(' 2022-01-28 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_01_29 VALUES (TO_DATE(' 2022-01-29 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_01_31 VALUES (TO_DATE(' 2022-01-31 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_02_01 VALUES (TO_DATE(' 2022-02-01 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_02_02 VALUES (TO_DATE(' 2022-02-02 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_02_03 VALUES (TO_DATE(' 2022-02-03 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_02_04 VALUES (TO_DATE(' 2022-02-04 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_02_07 VALUES (TO_DATE(' 2022-02-07 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_02_08 VALUES (TO_DATE(' 2022-02-08 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_02_09 VALUES (TO_DATE(' 2022-02-09 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_02_10 VALUES (TO_DATE(' 2022-02-10 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_02_11 VALUES (TO_DATE(' 2022-02-11 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_02_12 VALUES (TO_DATE(' 2022-02-12 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_02_14 VALUES (TO_DATE(' 2022-02-14 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_02_15 VALUES (TO_DATE(' 2022-02-15 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_02_16 VALUES (TO_DATE(' 2022-02-16 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_02_17 VALUES (TO_DATE(' 2022-02-17 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_02_18 VALUES (TO_DATE(' 2022-02-18 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_02_19 VALUES (TO_DATE(' 2022-02-19 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_02_21 VALUES (TO_DATE(' 2022-02-21 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_02_22 VALUES (TO_DATE(' 2022-02-22 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_02_23 VALUES (TO_DATE(' 2022-02-23 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_02_24 VALUES (TO_DATE(' 2022-02-24 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_02_25 VALUES (TO_DATE(' 2022-02-25 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_02_26 VALUES (TO_DATE(' 2022-02-26 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_02_28 VALUES (TO_DATE(' 2022-02-28 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_03_01 VALUES (TO_DATE(' 2022-03-01 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_03_02 VALUES (TO_DATE(' 2022-03-02 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_03_03 VALUES (TO_DATE(' 2022-03-03 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_03_04 VALUES (TO_DATE(' 2022-03-04 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_03_05 VALUES (TO_DATE(' 2022-03-05 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_03_07 VALUES (TO_DATE(' 2022-03-07 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_03_08 VALUES (TO_DATE(' 2022-03-08 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_03_09 VALUES (TO_DATE(' 2022-03-09 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_03_10 VALUES (TO_DATE(' 2022-03-10 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_03_11 VALUES (TO_DATE(' 2022-03-11 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_03_12 VALUES (TO_DATE(' 2022-03-12 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_03_14 VALUES (TO_DATE(' 2022-03-14 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_03_15 VALUES (TO_DATE(' 2022-03-15 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_03_16 VALUES (TO_DATE(' 2022-03-16 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_03_17 VALUES (TO_DATE(' 2022-03-17 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_03_18 VALUES (TO_DATE(' 2022-03-18 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_03_21 VALUES (TO_DATE(' 2022-03-21 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_03_22 VALUES (TO_DATE(' 2022-03-22 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_03_23 VALUES (TO_DATE(' 2022-03-23 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_03_24 VALUES (TO_DATE(' 2022-03-24 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_03_25 VALUES (TO_DATE(' 2022-03-25 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_03_26 VALUES (TO_DATE(' 2022-03-26 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_03_28 VALUES (TO_DATE(' 2022-03-28 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_03_29 VALUES (TO_DATE(' 2022-03-29 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_03_30 VALUES (TO_DATE(' 2022-03-30 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_03_31 VALUES (TO_DATE(' 2022-03-31 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_04_01 VALUES (TO_DATE(' 2022-04-01 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_04_04 VALUES (TO_DATE(' 2022-04-04 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_04_05 VALUES (TO_DATE(' 2022-04-05 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_04_06 VALUES (TO_DATE(' 2022-04-06 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_04_07 VALUES (TO_DATE(' 2022-04-07 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_04_08 VALUES (TO_DATE(' 2022-04-08 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_04_11 VALUES (TO_DATE(' 2022-04-11 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_04_12 VALUES (TO_DATE(' 2022-04-12 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_04_13 VALUES (TO_DATE(' 2022-04-13 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_04_14 VALUES (TO_DATE(' 2022-04-14 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_04_15 VALUES (TO_DATE(' 2022-04-15 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_04_16 VALUES (TO_DATE(' 2022-04-16 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_04_18 VALUES (TO_DATE(' 2022-04-18 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_04_19 VALUES (TO_DATE(' 2022-04-19 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_04_20 VALUES (TO_DATE(' 2022-04-20 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_04_21 VALUES (TO_DATE(' 2022-04-21 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_04_22 VALUES (TO_DATE(' 2022-04-22 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_04_23 VALUES (TO_DATE(' 2022-04-23 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_04_25 VALUES (TO_DATE(' 2022-04-25 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_04_26 VALUES (TO_DATE(' 2022-04-26 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_04_27 VALUES (TO_DATE(' 2022-04-27 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_04_28 VALUES (TO_DATE(' 2022-04-28 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_04_29 VALUES (TO_DATE(' 2022-04-29 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_04_30 VALUES (TO_DATE(' 2022-04-30 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_05_02 VALUES (TO_DATE(' 2022-05-02 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_05_03 VALUES (TO_DATE(' 2022-05-03 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_05_04 VALUES (TO_DATE(' 2022-05-04 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_05_05 VALUES (TO_DATE(' 2022-05-05 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_05_06 VALUES (TO_DATE(' 2022-05-06 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_05_07 VALUES (TO_DATE(' 2022-05-07 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_05_09 VALUES (TO_DATE(' 2022-05-09 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_05_10 VALUES (TO_DATE(' 2022-05-10 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_05_11 VALUES (TO_DATE(' 2022-05-11 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_05_12 VALUES (TO_DATE(' 2022-05-12 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_05_13 VALUES (TO_DATE(' 2022-05-13 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_05_14 VALUES (TO_DATE(' 2022-05-14 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_05_16 VALUES (TO_DATE(' 2022-05-16 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_05_17 VALUES (TO_DATE(' 2022-05-17 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_05_18 VALUES (TO_DATE(' 2022-05-18 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_05_19 VALUES (TO_DATE(' 2022-05-19 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_05_20 VALUES (TO_DATE(' 2022-05-20 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_05_21 VALUES (TO_DATE(' 2022-05-21 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_05_23 VALUES (TO_DATE(' 2022-05-23 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_05_24 VALUES (TO_DATE(' 2022-05-24 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_05_25 VALUES (TO_DATE(' 2022-05-25 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_05_26 VALUES (TO_DATE(' 2022-05-26 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_05_27 VALUES (TO_DATE(' 2022-05-27 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_05_28 VALUES (TO_DATE(' 2022-05-28 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_05_30 VALUES (TO_DATE(' 2022-05-30 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_05_31 VALUES (TO_DATE(' 2022-05-31 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_06_01 VALUES (TO_DATE(' 2022-06-01 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_06_02 VALUES (TO_DATE(' 2022-06-02 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_06_03 VALUES (TO_DATE(' 2022-06-03 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_06_04 VALUES (TO_DATE(' 2022-06-04 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_06_06 VALUES (TO_DATE(' 2022-06-06 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_06_07 VALUES (TO_DATE(' 2022-06-07 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_06_08 VALUES (TO_DATE(' 2022-06-08 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_06_09 VALUES (TO_DATE(' 2022-06-09 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_06_10 VALUES (TO_DATE(' 2022-06-10 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_06_11 VALUES (TO_DATE(' 2022-06-11 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_06_13 VALUES (TO_DATE(' 2022-06-13 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_06_14 VALUES (TO_DATE(' 2022-06-14 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_06_15 VALUES (TO_DATE(' 2022-06-15 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_06_16 VALUES (TO_DATE(' 2022-06-16 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_06_17 VALUES (TO_DATE(' 2022-06-17 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_06_18 VALUES (TO_DATE(' 2022-06-18 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_06_20 VALUES (TO_DATE(' 2022-06-20 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_06_21 VALUES (TO_DATE(' 2022-06-21 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_06_22 VALUES (TO_DATE(' 2022-06-22 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_06_23 VALUES (TO_DATE(' 2022-06-23 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_06_24 VALUES (TO_DATE(' 2022-06-24 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_06_25 VALUES (TO_DATE(' 2022-06-25 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_06_27 VALUES (TO_DATE(' 2022-06-27 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_06_28 VALUES (TO_DATE(' 2022-06-28 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_06_29 VALUES (TO_DATE(' 2022-06-29 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_06_30 VALUES (TO_DATE(' 2022-06-30 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_07_01 VALUES (TO_DATE(' 2022-07-01 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_07_02 VALUES (TO_DATE(' 2022-07-02 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_07_04 VALUES (TO_DATE(' 2022-07-04 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_07_05 VALUES (TO_DATE(' 2022-07-05 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_07_06 VALUES (TO_DATE(' 2022-07-06 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_07_07 VALUES (TO_DATE(' 2022-07-07 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_07_08 VALUES (TO_DATE(' 2022-07-08 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_07_09 VALUES (TO_DATE(' 2022-07-09 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_07_11 VALUES (TO_DATE(' 2022-07-11 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_07_12 VALUES (TO_DATE(' 2022-07-12 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_07_13 VALUES (TO_DATE(' 2022-07-13 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_07_14 VALUES (TO_DATE(' 2022-07-14 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_07_15 VALUES (TO_DATE(' 2022-07-15 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_07_16 VALUES (TO_DATE(' 2022-07-16 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_07_18 VALUES (TO_DATE(' 2022-07-18 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_07_19 VALUES (TO_DATE(' 2022-07-19 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_07_20 VALUES (TO_DATE(' 2022-07-20 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_07_21 VALUES (TO_DATE(' 2022-07-21 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_07_22 VALUES (TO_DATE(' 2022-07-22 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_07_23 VALUES (TO_DATE(' 2022-07-23 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_07_25 VALUES (TO_DATE(' 2022-07-25 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_07_26 VALUES (TO_DATE(' 2022-07-26 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_07_27 VALUES (TO_DATE(' 2022-07-27 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_07_28 VALUES (TO_DATE(' 2022-07-28 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_07_29 VALUES (TO_DATE(' 2022-07-29 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_07_30 VALUES (TO_DATE(' 2022-07-30 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_08_01 VALUES (TO_DATE(' 2022-08-01 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_08_02 VALUES (TO_DATE(' 2022-08-02 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_08_03 VALUES (TO_DATE(' 2022-08-03 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_08_04 VALUES (TO_DATE(' 2022-08-04 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_08_05 VALUES (TO_DATE(' 2022-08-05 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_08_06 VALUES (TO_DATE(' 2022-08-06 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_08_08 VALUES (TO_DATE(' 2022-08-08 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_08_09 VALUES (TO_DATE(' 2022-08-09 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_08_10 VALUES (TO_DATE(' 2022-08-10 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_08_11 VALUES (TO_DATE(' 2022-08-11 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_08_12 VALUES (TO_DATE(' 2022-08-12 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_08_13 VALUES (TO_DATE(' 2022-08-13 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_08_15 VALUES (TO_DATE(' 2022-08-15 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_08_16 VALUES (TO_DATE(' 2022-08-16 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_08_17 VALUES (TO_DATE(' 2022-08-17 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_08_18 VALUES (TO_DATE(' 2022-08-18 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_08_19 VALUES (TO_DATE(' 2022-08-19 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_08_20 VALUES (TO_DATE(' 2022-08-20 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_08_22 VALUES (TO_DATE(' 2022-08-22 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_08_23 VALUES (TO_DATE(' 2022-08-23 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_08_24 VALUES (TO_DATE(' 2022-08-24 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_08_25 VALUES (TO_DATE(' 2022-08-25 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_08_26 VALUES (TO_DATE(' 2022-08-26 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_08_27 VALUES (TO_DATE(' 2022-08-27 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_08_29 VALUES (TO_DATE(' 2022-08-29 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_08_30 VALUES (TO_DATE(' 2022-08-30 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_08_31 VALUES (TO_DATE(' 2022-08-31 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_09_01 VALUES (TO_DATE(' 2022-09-01 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_09_02 VALUES (TO_DATE(' 2022-09-02 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_09_03 VALUES (TO_DATE(' 2022-09-03 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_09_05 VALUES (TO_DATE(' 2022-09-05 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_09_06 VALUES (TO_DATE(' 2022-09-06 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_09_07 VALUES (TO_DATE(' 2022-09-07 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_09_08 VALUES (TO_DATE(' 2022-09-08 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_09_09 VALUES (TO_DATE(' 2022-09-09 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_09_10 VALUES (TO_DATE(' 2022-09-10 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_09_12 VALUES (TO_DATE(' 2022-09-12 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_09_13 VALUES (TO_DATE(' 2022-09-13 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_09_14 VALUES (TO_DATE(' 2022-09-14 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_09_15 VALUES (TO_DATE(' 2022-09-15 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_09_16 VALUES (TO_DATE(' 2022-09-16 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_09_17 VALUES (TO_DATE(' 2022-09-17 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_09_19 VALUES (TO_DATE(' 2022-09-19 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_09_20 VALUES (TO_DATE(' 2022-09-20 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_09_21 VALUES (TO_DATE(' 2022-09-21 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_09_22 VALUES (TO_DATE(' 2022-09-22 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_09_23 VALUES (TO_DATE(' 2022-09-23 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_09_24 VALUES (TO_DATE(' 2022-09-24 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_09_26 VALUES (TO_DATE(' 2022-09-26 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_09_27 VALUES (TO_DATE(' 2022-09-27 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_09_28 VALUES (TO_DATE(' 2022-09-28 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_09_29 VALUES (TO_DATE(' 2022-09-29 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_09_30 VALUES (TO_DATE(' 2022-09-30 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_10_01 VALUES (TO_DATE(' 2022-10-01 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_10_03 VALUES (TO_DATE(' 2022-10-03 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_10_04 VALUES (TO_DATE(' 2022-10-04 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_10_05 VALUES (TO_DATE(' 2022-10-05 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_10_06 VALUES (TO_DATE(' 2022-10-06 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_10_07 VALUES (TO_DATE(' 2022-10-07 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_10_08 VALUES (TO_DATE(' 2022-10-08 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_10_10 VALUES (TO_DATE(' 2022-10-10 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_10_11 VALUES (TO_DATE(' 2022-10-11 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_10_12 VALUES (TO_DATE(' 2022-10-12 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_10_13 VALUES (TO_DATE(' 2022-10-13 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_10_14 VALUES (TO_DATE(' 2022-10-14 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_10_15 VALUES (TO_DATE(' 2022-10-15 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_10_17 VALUES (TO_DATE(' 2022-10-17 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_10_18 VALUES (TO_DATE(' 2022-10-18 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_10_19 VALUES (TO_DATE(' 2022-10-19 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_10_20 VALUES (TO_DATE(' 2022-10-20 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_10_21 VALUES (TO_DATE(' 2022-10-21 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_10_24 VALUES (TO_DATE(' 2022-10-24 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_10_25 VALUES (TO_DATE(' 2022-10-25 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_10_26 VALUES (TO_DATE(' 2022-10-26 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_10_27 VALUES (TO_DATE(' 2022-10-27 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_10_28 VALUES (TO_DATE(' 2022-10-28 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_10_29 VALUES (TO_DATE(' 2022-10-29 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_10_31 VALUES (TO_DATE(' 2022-10-31 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_11_01 VALUES (TO_DATE(' 2022-11-01 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_11_02 VALUES (TO_DATE(' 2022-11-02 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_11_03 VALUES (TO_DATE(' 2022-11-03 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_11_04 VALUES (TO_DATE(' 2022-11-04 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_11_05 VALUES (TO_DATE(' 2022-11-05 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_11_07 VALUES (TO_DATE(' 2022-11-07 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_11_08 VALUES (TO_DATE(' 2022-11-08 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_11_09 VALUES (TO_DATE(' 2022-11-09 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_11_10 VALUES (TO_DATE(' 2022-11-10 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_11_11 VALUES (TO_DATE(' 2022-11-11 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_11_12 VALUES (TO_DATE(' 2022-11-12 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_11_14 VALUES (TO_DATE(' 2022-11-14 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_11_15 VALUES (TO_DATE(' 2022-11-15 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_11_16 VALUES (TO_DATE(' 2022-11-16 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_11_17 VALUES (TO_DATE(' 2022-11-17 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_11_18 VALUES (TO_DATE(' 2022-11-18 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_11_19 VALUES (TO_DATE(' 2022-11-19 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_11_21 VALUES (TO_DATE(' 2022-11-21 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC,  
  PARTITION DATE_2022_11_22 VALUES (TO_DATE(' 2022-11-22 00:00:00', 'syyyy-mm-dd hh24:mi:ss', 'nls_calendar=gregorian'))
    COMPRESS BASIC
)
/

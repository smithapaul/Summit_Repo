CREATE OR REPLACE PROCEDURE             "UM_D_TRANSFER_DICT_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table UM_D_TRANSFER_DICT from PeopleSoft table UM_D_TRANSFER_DICT.
--
--V01  SMT-xxxx 03/12/2018,     James Doucette
--                              Converted from DataStage
--V01   SMT-xxxx 03/15/2018,    James Doucette
--                              Updated INSTITUTION field to INSTITUTION_CD.
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_D_TRANSFER_DICT';
        intProcessSid                   Integer;
        dtProcessStart                  Date            := SYSDATE;
        strMessage01                    Varchar2(4000);
        strMessage02                    Varchar2(512);
        strMessage03                    Varchar2(512)   :='';
        strNewLine                      Varchar2(2)     := chr(13) || chr(10);
        strSqlCommand                   Varchar2(32767) :='';
        strSqlDynamic                   Varchar2(32767) :='';
        strClientInfo                   Varchar2(100);
        intRowCount                     Integer;
        intTotalRowCount                Integer         := 0;
        numSqlCode                      Number;
        strSqlErrm                      Varchar2(4000);
        intTries                        Integer;

BEGIN

strSqlCommand := 'DBMS_APPLICATION_INFO.SET_CLIENT_INFO';
DBMS_APPLICATION_INFO.SET_CLIENT_INFO (strProcessName);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_INIT';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_INIT
        (
                i_MartId                => strMartId,
                i_ProcessName           => strProcessName,
                i_ProcessStartTime      => dtProcessStart,
                o_ProcessSid            => intProcessSid
        );

strMessage01    := 'Truncating table CSMRT_OWNER.UM_D_TRANSFER_DICT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_D_TRANSFER_DICT';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_D_TRANSFER_DICT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_D_TRANSFER_DICT');

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_D_TRANSFER_DICT disable constraint PK_UM_D_TRANSFER_DICT';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
strMessage01    := 'Inserting data into CSMRT_OWNER.UM_D_TRANSFER_DICT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_D_TRANSFER_DICT';				
insert /*+ append enable_parallel_dml parallel(8) */ into UM_D_TRANSFER_DICT 
  with X as ( 
select /*+ inline parallel(8) */ 
       FIELDNAME, FIELDVALUE, EFFDT, SRC_SYS_ID, 
       XLATLONGNAME, XLATSHORTNAME, DATA_ORIGIN, 
       row_number() over (partition by FIELDNAME, FIELDVALUE, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1800') else EFFDT end) desc) X_ORDER
  from CSSTG_OWNER.PSXLATITEM
 where DATA_ORIGIN <> 'D'),
       Q1 as (  
select /*+ inline parallel(8) */ 
       INSTITUTION INSTITUTION_CD, TRNSFR_SRC_ID, COMP_SUBJECT_AREA, SRC_SYS_ID, 
       EFFDT, EFF_STATUS, DESCR TRNSFR_SUBJ_DESCR, 
       TC_CATLG_ORG_TYPE, TRNSFR_GRADE_FL TRNSFR_GRADE_FLG,   
       DATA_ORIGIN,
       row_number() over (partition by INSTITUTION, TRNSFR_SRC_ID, COMP_SUBJECT_AREA, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1800') else EFFDT end) desc) Q_ORDER
  from CSSTG_OWNER.PS_TRNSFR_SUBJ), 
       Q2 as (  
select /*+ inline parallel(8) */ 
       INSTITUTION INSTITUTION_CD, TRNSFR_SRC_ID, COMP_SUBJECT_AREA, TRNSFR_EQVLNCY_CMP, SRC_SYS_ID, 
       EFFDT, DESCR TRNSFR_COMP_DESCR, 
       EXT_TERM_TYPE, TRNSFR_CRSE_FL TRNSFR_CRSE_FLG, INP_CRSE_CNT, UNT_TRNSFR_SRC, XS_CRSE_FL XS_CRSE_FLG,
       DATA_ORIGIN,
       row_number() over (partition by INSTITUTION, TRNSFR_SRC_ID, COMP_SUBJECT_AREA, TRNSFR_EQVLNCY_CMP, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1800') else EFFDT end) desc) Q_ORDER
  from CSSTG_OWNER.PS_TRNSFR_COMP), 
       Q3 as (  
select /*+ inline parallel(8) */ 
       INSTITUTION INSTITUTION_CD, TRNSFR_SRC_ID, COMP_SUBJECT_AREA, TRNSFR_EQVLNCY_CMP, TRNSFR_CMP_SEQ, SRC_SYS_ID,
       EFFDT, CRSE_ID, CRSE_OFFER_NBR EXT_CRSE_OFFER_NBR, SCHOOL_SUBJECT, SCHOOL_CRSE_NBR, 
       UNITS_MINIMUM, UNITS_MAXIMUM, GRADE_PTS_MIN, GRADE_PTS_MAX, SSR_MAX_AGE, TRNSFR_GRADE_FL TRNSFR_GRADE_FLG, 
       DATA_ORIGIN, 
       row_number() over (partition by INSTITUTION, TRNSFR_SRC_ID, COMP_SUBJECT_AREA, TRNSFR_EQVLNCY_CMP, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1800') else EFFDT end) desc, TRNSFR_CMP_SEQ desc) Q_ORDER
  from CSSTG_OWNER.PS_TRNSFR_FROM), 
       Q4 as (  
select /*+ inline parallel(8) */ 
       INSTITUTION INSTITUTION_CD, TRNSFR_SRC_ID, COMP_SUBJECT_AREA, TRNSFR_EQVLNCY_CMP, CRSE_ID UM_CRSE_ID, SRC_SYS_ID,
       EFFDT, CRSE_OFFER_NBR UM_CRSE_OFFER_NBR, UNT_TAKEN UM_UNIT_TAKEN, SSR_TR_DEF_GRD_TYP UM_SSR_TR_DEF_GRD_TYP, SSR_TR_DEF_GRD_SEQ UM_SSR_TR_DEF_GRD_SEQ, 
       DATA_ORIGIN, 
       row_number() over (partition by INSTITUTION, TRNSFR_SRC_ID, COMP_SUBJECT_AREA, TRNSFR_EQVLNCY_CMP, CRSE_ID, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1800') else EFFDT end) desc) Q_ORDER
  from CSSTG_OWNER.PS_TRNSFR_TO), 
       S as (
select /*+ inline parallel(8) */ 
       Q1.INSTITUTION_CD, Q1.TRNSFR_SRC_ID, Q1.COMP_SUBJECT_AREA, Q2.TRNSFR_EQVLNCY_CMP, Q3.TRNSFR_CMP_SEQ, nvl(Q4.UM_CRSE_ID,'-') CRSE_ID, Q1.SRC_SYS_ID, 
       Q1.EFFDT, Q1.EFF_STATUS,  
       Q1.TRNSFR_SUBJ_DESCR, Q2.TRNSFR_COMP_DESCR, 
       nvl(C.CRSE_SID,2147483646) CRSE_SID, 
       nvl(E.EXT_CRSE_SID,2147483646) EXT_CRSE_SID, 
       nvl(O.EXT_ORG_SID,2147483646) EXT_ORG_SID, 
       Q3.EXT_CRSE_OFFER_NBR, Q2.EXT_TERM_TYPE, nvl(X1.XLATSHORTNAME,'-') EXT_TERM_TYPE_SD, nvl(X1.XLATLONGNAME,'-') EXT_TERM_TYPE_LD, 
       Q3.GRADE_PTS_MIN, Q3.GRADE_PTS_MAX, Q2.INP_CRSE_CNT, Q1.TC_CATLG_ORG_TYPE INT_TRANSFER_FLG, Q3.SCHOOL_SUBJECT, Q3.SCHOOL_CRSE_NBR, Q3.SSR_MAX_AGE, 
       Q2.TRNSFR_CRSE_FLG, (case when Q4.UM_CRSE_ID is NULL then 'REJECTED' else 'APPROVED' end) TRNSFR_CRSE_STATUS,    -- Always 'APPROVED' if inner join to Q4???  
       Q3.TRNSFR_GRADE_FLG, nvl(Q4.UM_CRSE_ID,'-') UM_CRSE_ID, nvl(Q4.UM_CRSE_OFFER_NBR,0) UM_CRSE_OFFER_NBR, 
       nvl(Q4.UM_SSR_TR_DEF_GRD_TYP,'-') UM_SSR_TR_DEF_GRD_TYP, nvl(X3.XLATSHORTNAME,'-') UM_SSR_TR_DEF_GRD_TYP_SD, nvl(X3.XLATLONGNAME,'-') UM_SSR_TR_DEF_GRD_TYP_LD, 
       nvl(Q4.UM_SSR_TR_DEF_GRD_SEQ,'-') UM_SSR_TR_DEF_GRD_SEQ, nvl(Q4.UM_UNIT_TAKEN,0) UM_UNIT_TAKEN, Q3.UNITS_MINIMUM, Q3.UNITS_MAXIMUM, 
       Q2.UNT_TRNSFR_SRC, nvl(X4.XLATSHORTNAME,'-') UNT_TRNSFR_SRC_SD, nvl(X4.XLATLONGNAME,'-') UNT_TRNSFR_SRC_LD, 
       Q2.XS_CRSE_FLG, 
       least(Q1.DATA_ORIGIN,Q2.DATA_ORIGIN,Q3.DATA_ORIGIN,nvl(Q4.DATA_ORIGIN,'Z')) DATA_ORIGIN  
  from Q1
  join Q2
    on Q1.INSTITUTION_CD = Q2.INSTITUTION_CD
   and Q1.TRNSFR_SRC_ID = Q2.TRNSFR_SRC_ID
   and Q1.COMP_SUBJECT_AREA = Q2.COMP_SUBJECT_AREA
   and Q1.SRC_SYS_ID = Q2.SRC_SYS_ID
   and Q2.Q_ORDER = 1
  join Q3
    on Q2.INSTITUTION_CD = Q3.INSTITUTION_CD
   and Q2.TRNSFR_SRC_ID = Q3.TRNSFR_SRC_ID
   and Q2.COMP_SUBJECT_AREA = Q3.COMP_SUBJECT_AREA
   and Q2.TRNSFR_EQVLNCY_CMP = Q3.TRNSFR_EQVLNCY_CMP  
   and Q2.EFFDT = Q3.EFFDT      -- Need??? 
   and Q2.SRC_SYS_ID = Q3.SRC_SYS_ID
   and Q3.Q_ORDER = 1
  join Q4               -- outer join???  
    on Q3.INSTITUTION_CD = Q4.INSTITUTION_CD
   and Q3.TRNSFR_SRC_ID = Q4.TRNSFR_SRC_ID
   and Q3.COMP_SUBJECT_AREA = Q4.COMP_SUBJECT_AREA
   and Q3.TRNSFR_EQVLNCY_CMP = Q4.TRNSFR_EQVLNCY_CMP  
   and Q3.SRC_SYS_ID = Q4.SRC_SYS_ID
   and Q4.Q_ORDER = 1
  left outer join UM_D_CRSE C
    on Q4.UM_CRSE_ID = C.CRSE_CD
   and Q4.UM_CRSE_OFFER_NBR = C.CRSE_OFFER_NUM
   and Q4.SRC_SYS_ID = C.SRC_SYS_ID
   and C.DATA_ORIGIN <> 'D'
  left outer join UM_D_EXT_CRSE E
    on Q3.TRNSFR_SRC_ID = E.EXT_ORG_ID 
   and Q3.SCHOOL_SUBJECT = E.SCHOOL_SUBJECT 
   and Q3.SCHOOL_CRSE_NBR = E.SCHOOL_CRSE_NBR 
   and Q3.SRC_SYS_ID = E.SRC_SYS_ID
   and E.DATA_ORIGIN <> 'D'
  left outer join PS_D_EXT_ORG O
    on Q3.TRNSFR_SRC_ID = O.EXT_ORG_ID 
   and Q3.SRC_SYS_ID = O.SRC_SYS_ID
   and O.DATA_ORIGIN <> 'D'
  left outer join X X1
    on Q2.EXT_TERM_TYPE = X1.FIELDVALUE
   and Q2.SRC_SYS_ID = X1.SRC_SYS_ID
   and X1.FIELDNAME = 'EXT_TERM_TYPE'
   and X1.X_ORDER = 1
  left outer join X X3
    on Q4.UM_SSR_TR_DEF_GRD_TYP = X3.FIELDVALUE
   and Q4.SRC_SYS_ID = X3.SRC_SYS_ID
   and X3.FIELDNAME = 'SSR_TR_DEF_GRD_TYP'
   and X3.X_ORDER = 1
  left outer join X X4
    on Q2.UNT_TRNSFR_SRC = X4.FIELDVALUE
   and Q2.SRC_SYS_ID = X4.SRC_SYS_ID
   and X4.FIELDNAME = 'UNT_TRNSFR_SRC'
   and X4.X_ORDER = 1
 where Q1.Q_ORDER = 1
 )
select /*+ parallel(8) */
       ROWNUM TRNSFR_DICT_SID, INSTITUTION_CD, TRNSFR_SRC_ID, COMP_SUBJECT_AREA, TRNSFR_EQVLNCY_CMP, TRNSFR_CMP_SEQ, CRSE_ID, SRC_SYS_ID, 
       EFFDT, EFF_STATUS, TRNSFR_SUBJ_DESCR, TRNSFR_COMP_DESCR, CRSE_SID, EXT_CRSE_SID, EXT_ORG_SID, EXT_CRSE_OFFER_NBR, EXT_TERM_TYPE, EXT_TERM_TYPE_SD, EXT_TERM_TYPE_LD, 
       GRADE_PTS_MIN, GRADE_PTS_MAX, INP_CRSE_CNT, INT_TRANSFER_FLG, SCHOOL_SUBJECT, SCHOOL_CRSE_NBR, SSR_MAX_AGE, TRNSFR_CRSE_FLG, TRNSFR_CRSE_STATUS, TRNSFR_GRADE_FLG, 
       UM_CRSE_ID, UM_CRSE_OFFER_NBR, UM_SSR_TR_DEF_GRD_TYP, UM_SSR_TR_DEF_GRD_TYP_SD, UM_SSR_TR_DEF_GRD_TYP_LD, UM_SSR_TR_DEF_GRD_SEQ, 
       UM_UNIT_TAKEN, UNITS_MINIMUM, UNITS_MAXIMUM, UNT_TRNSFR_SRC, UNT_TRNSFR_SRC_SD, UNT_TRNSFR_SRC_LD, XS_CRSE_FLG, 
       'S' DATA_ORIGIN, SYSDATE CREATED_EW_DTTM, SYSDATE LASTUPD_EW_DTTM
  from S
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_D_TRANSFER_DICT rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_D_TRANSFER_DICT',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_D_TRANSFER_DICT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_D_TRANSFER_DICT enable constraint PK_UM_D_TRANSFER_DICT';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_D_TRANSFER_DICT');

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

strMessage01    := strProcessName || ' is complete.';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

EXCEPTION
    WHEN OTHERS THEN
        COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_EXCEPTION
                (
                        i_SqlCommand   => strSqlCommand,
                        i_SqlCode      => SQLCODE,
                        i_SqlErrm      => SQLERRM
                );

END UM_D_TRANSFER_DICT_P;
/

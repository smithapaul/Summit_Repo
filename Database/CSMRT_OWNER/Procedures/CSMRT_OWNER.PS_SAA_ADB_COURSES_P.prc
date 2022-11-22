DROP PROCEDURE CSMRT_OWNER.PS_SAA_ADB_COURSES_P
/

--
-- PS_SAA_ADB_COURSES_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_SAA_ADB_COURSES_P" AUTHID CURRENT_USER IS


------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_SAA_ADB_COURSES from PeopleSoft table PS_SAA_ADB_COURSES.
--
-- V01  SMT-xxxx 08/01/2017,    Jim Doucette
--                              Converted from PS_SAA_ADB_COURSES.SQL
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_SAA_ADB_COURSES';
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

strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update START_DT on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Reading',
       START_DT = sysdate,
       END_DT = NULL
 where TABLE_NAME = 'PS_SAA_ADB_COURSES'
;

strSqlCommand := 'commit';
commit;

strSqlCommand   := 'update START_DT on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Disable',
       START_DT = sysdate,
       END_DT = NULL
 where TABLE_NAME = 'PS_SAA_ADB_COURSES'
;

strSqlCommand := 'commit';
commit;

strSqlCommand  := 'alter table CSSTG_OWNER.PS_SAA_ADB_COURSES disable constraint PK_PS_SAA_ADB_COURSES';
begin
execute immediate 'alter table CSSTG_OWNER.PS_SAA_ADB_COURSES disable constraint PK_PS_SAA_ADB_COURSES';
end;


strSqlCommand := 'commit';
commit;


strSqlCommand   := 'truncate table CSSTG_OWNER.PS_SAA_ADB_COURSES';
begin
execute immediate 'truncate table CSSTG_OWNER.PS_SAA_ADB_COURSES';
end;


strSqlCommand := 'commit';
commit;



strMessage01    := 'Loading data into CSSTG_OWNER.PS_SAA_ADB_COURSES';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'Insert into CSSTG_OWNER.PS_SAA_ADB_COURSES';
insert /*+ append */  into CSSTG_OWNER.PS_SAA_ADB_COURSES
select /*+ full(C) */
       EMPLID, 
       ANALYSIS_DB_SEQ, 
       SAA_CAREER_RPT, 
       CRSE_TAG, 
       'CS90' SRC_SYS_ID,
       RPT_DATE, 
       trim(INSTITUTION), 
       trim(ACAD_CAREER), 
       trim(CRSE_CAREER), 
       trim(STRM), 
       trim(CLASS_NBR), 
       trim(STDNT_ENRL_STATUS), 
       UNT_TAKEN, 
       UNT_EARNED, 
       UNT_PRGRSS, 
       trim(GRADING_BASIS_ENRL), 
       trim(CRSE_GRADE_OFF), 
       trim(REPEAT_CODE), 
       ASSOCIATED_CLASS, 
       trim(AUDIT_GRADE_BASIS), 
       trim(EARN_CREDIT), 
       trim(INCLUDE_IN_GPA), 
       trim(UNITS_ATTEMPTED), 
       GRADE_POINTS, 
       trim(CRSE_ID), 
       CRSE_OFFER_NBR, 
       trim(SESSION_CODE), 
       trim(CLASS_SECTION), 
       trim(ACAD_GROUP), 
       trim(SUBJECT), 
       trim(CATALOG_NBR), 
       trim(DESCR), 
       trim(SSR_COMPONENT), 
       CRS_TOPIC_ID, 
       trim(SAA_DISPLAY_TOPIC), 
       trim(EQUIV_CRSE_ID), 
       trim(OVRD_CRSE_EQUIV_ID), 
       START_DT, 
       END_DT, 
       CRSE_COUNT, 
       trim(CLASS_ENRL_TYPE), 
       trim(SAA_CRSE_TYPE), 
       trim(RQ_SCHOOL_TYPE), 
       trim(GRADE_CATEGORY), 
       trim(VALID_ATTEMPT), 
       GRD_PTS_PER_UNIT, 
       trim(RQMNT_DESIGNTN), 
       EVALUATION_DT, 
       TEST_DT, 
       trim(GRADING_SCHEME), 
       trim(SUB_ACAD_GROUP), 
       trim(SUB_CRSE_ID), 
       trim(SUB_SUBJECT), 
       trim(SUB_CATLG_NBR), 
       trim(SAA_SUB_DESCR), 
       trim(OPRID), 
       trim(SAA_DISPLAY_OPTION), 
       trim(STDNT_SUB_NBR), 
       trim(DESCR254A),
       sysdate CREATED_EW_DTTM
  from SYSADM.PS_SAA_ADB_COURSES@SASOURCE C 
 where TSCRPT_TYPE in ('DADV','LADV','DGPA','LGPA')
   and EMPLID between '00000000' and '99999999'
   and length(EMPLID) = 8
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_SAA_ADB_COURSES rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_SAA_ADB_COURSES',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Indexing CSSTG_OWNER.PS_SAA_ADB_COURSES';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Indexing',
       END_DT = NULL
 where TABLE_NAME = 'PS_SAA_ADB_COURSES'
;

strSqlCommand := 'commit';
commit;

strMessage01    := 'Enable contraint CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'enable constraint PK_PS_SAA_ADB_COURSES';
strSqlCommand  := 'alter table CSSTG_OWNER.PS_SAA_ADB_COURSES enable constraint PK_PS_SAA_ADB_COURSES';
begin
execute immediate 'alter table CSSTG_OWNER.PS_SAA_ADB_COURSES enable constraint PK_PS_SAA_ADB_COURSES';
end;


strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = sysdate,
       OLD_MAX_SCN = 0,
       NEW_MAX_SCN = 999999999999
 where TABLE_NAME = 'PS_SAA_ADB_COURSES'
;


strSqlCommand := 'commit';
commit;


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

END PS_SAA_ADB_COURSES_P;
/

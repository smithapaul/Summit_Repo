CREATE OR REPLACE PROCEDURE             "PS_R_AWD_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads table PS_R_AWD
--
 --V01  SMT-xxxx 01/15/2018,    Srikanth,Pabbu
--                              
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_R_AWD';
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

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.PS_R_AWD';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','PS_R_AWD');

strSqlDynamic   := 'alter table CSMRT_OWNER.PS_R_AWD disable constraint PK_PS_R_AWD';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
strMessage01    := 'Truncating table CSMRT_OWNER.PS_R_AWD';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.PS_R_AWD';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Inserting data into CSMRT_OWNER.PS_R_AWD';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.PS_R_AWD';				
insert /*+ append */ into PS_R_AWD 
 with Q1 as (  
select EMPLID PERSON_ID, SRC_SYS_ID, DT_RECVD AWD_RCVD_DT, INSTITUTION INSTITUTION_CD, 
       ACAD_CAREER ACAD_CAR_CD, STRM TERM_CD, AWARD_CODE AWD_CD, 
       ACAD_PROG ACAD_PROG_CD, ACAD_PLAN ACAD_PLAN_CD, DESCRFORMAL, GRANTOR,
       substr(COMMENTS,1,100) COMMENTS,
       row_number() over (partition by INSTITUTION, EMPLID, AWARD_CODE, DT_RECVD, SRC_SYS_ID
                              order by STRM desc, ACAD_CAREER) Q_ORDER 
  from CSSTG_OWNER.PS_HONOR_AWARD_CS 
 where DATA_ORIGIN <> 'D')
select P.PERSON_SID, A.AWD_SID, Q1.AWD_RCVD_DT, Q1.SRC_SYS_ID,
       Q1.PERSON_ID, Q1.INSTITUTION_CD, Q1.AWD_CD,
       Q1.ACAD_CAR_CD, '-' ACAD_CAR_SD,     -- Retired!!!
       Q1.ACAD_PROG_CD, '-' ACAD_PROG_SD,   -- Retired!!!
       Q1.ACAD_PLAN_CD, '-' ACAD_PLAN_SD,   -- Retired!!! 
       Q1.TERM_CD, '-' TERM_SD,             -- Retired!!!
       Q1.DESCRFORMAL, Q1.GRANTOR, Q1.COMMENTS, 
       to_date('01-JAN-1900') EFF_START_DT, to_date('31-DEC-9999') EFF_END_DT, 'Y' CURRENT_IND, 
       'N' LOAD_ERROR, 'S' DATA_ORIGIN, SYSDATE CREATED_EW_DTTM, SYSDATE LASTUPD_EW_DTTM, 1234 BATCH_SID
  from Q1
  join PS_D_PERSON P
    on Q1.PERSON_ID = P.PERSON_ID
   and Q1.SRC_SYS_ID = P.SRC_SYS_ID
   and P.DATA_ORIGIN <> 'D'
  join PS_D_AWD A
    on Q1.INSTITUTION_CD = A.INSTITUTION_CD
   and Q1.AWD_CD = A.AWD_CD
   and Q1.SRC_SYS_ID = A.SRC_SYS_ID
   and A.DATA_ORIGIN <> 'D'
 where Q1.Q_ORDER = 1;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_R_AWD rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_R_AWD',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.PS_R_AWD';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'alter table CSMRT_OWNER.PS_R_AWD enable constraint PK_PS_R_AWD';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','PS_R_AWD');

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

END PS_R_AWD_P;
/

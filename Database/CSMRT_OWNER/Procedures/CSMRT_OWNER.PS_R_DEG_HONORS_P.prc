DROP PROCEDURE CSMRT_OWNER.PS_R_DEG_HONORS_P
/

--
-- PS_R_DEG_HONORS_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_R_DEG_HONORS_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads table PS_R_DEG_HONORS.
--
 --V01  SMT-xxxx 01/15/2018,    Srikanth,Pabbu
--                              
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_R_DEG_HONORS';
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

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.PS_R_DEG_HONORS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','PS_R_DEG_HONORS');

strSqlDynamic   := 'alter table CSMRT_OWNER.PS_R_DEG_HONORS disable constraint PK_PS_R_DEG_HONORS';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
strMessage01    := 'Truncating table CSMRT_OWNER.PS_R_DEG_HONORS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.PS_R_DEG_HONORS';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Inserting data into CSMRT_OWNER.PS_R_DEG_HONORS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.PS_R_DEG_HONORS';				
insert /*+ append */ into PS_R_DEG_HONORS 
 with Q1 as (  
select EMPLID PERSON_ID, STDNT_DEGR STDNT_DEGR_CD, SRC_SYS_ID,
       DEGREE, INSTITUTION INSTITUTION_CD, DEGR_CONFER_DT 
   from CSSTG_OWNER.PS_ACAD_DEGR 
 where DATA_ORIGIN <> 'D'), 
       Q2 as (  
select EMPLID PERSON_ID, STDNT_DEGR STDNT_DEGR_CD, HONORS_NBR HONORS_NUM, SRC_SYS_ID,
       HONORS_CODE HONORS_CD, HONORS_AWARD_DT HONORS_AWD_DT
   from CSSTG_OWNER.PS_ACAD_DEGR_HONS 
 where DATA_ORIGIN <> 'D')
select P.PERSON_SID, Q1.STDNT_DEGR_CD, Q2.HONORS_NUM, Q1.SRC_SYS_ID, 
       Q1.PERSON_ID, nvl(A.DEG_SID,2147483646) DEG_SID, nvl(H.DEG_HONORS_SID,2147483646) DEG_HONORS_SID, Q2.HONORS_AWD_DT, 
       to_date('01-JAN-1900') EFF_START_DT, to_date('31-DEC-9999') EFF_END_DT, 'Y' CURRENT_IND, 
       'N' LOAD_ERROR, 'S' DATA_ORIGIN, SYSDATE CREATED_EW_DTTM, SYSDATE LASTUPD_EW_DTTM, 1234 BATCH_SID
  from Q1
  join Q2
    on Q1.PERSON_ID = Q2.PERSON_ID 
   and Q1.STDNT_DEGR_CD = Q2.STDNT_DEGR_CD
   and Q1.SRC_SYS_ID = Q2.SRC_SYS_ID
  join PS_D_PERSON P
    on Q1.PERSON_ID = P.PERSON_ID
   and Q1.SRC_SYS_ID = P.SRC_SYS_ID
   and P.DATA_ORIGIN <> 'D'
  left outer join PS_D_DEG A
    on Q1.DEGREE = A.DEG_CD
   and Q1.SRC_SYS_ID = A.SRC_SYS_ID
   and A.DATA_ORIGIN <> 'D'
  left outer join PS_D_DEG_HONORS H
    on Q1.INSTITUTION_CD = H.INSTITUTION_CD 
   and 'DH' = H.HONORS_TYPE_CD 
   and Q2.HONORS_CD = H.HONORS_CD
   and Q1.SRC_SYS_ID = H.SRC_SYS_ID
   and H.DATA_ORIGIN <> 'D'
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_R_DEG_HONORS rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_R_DEG_HONORS',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.PS_R_DEG_HONORS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'alter table CSMRT_OWNER.PS_R_DEG_HONORS enable constraint PK_PS_R_DEG_HONORS';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','PS_R_DEG_HONORS');

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

END PS_R_DEG_HONORS_P;
/

CREATE OR REPLACE PROCEDURE             "UM_F_SF_STDNT_EQUTN_VAR_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table UM_F_SF_STDNT_EQUTN_VAR from PeopleSoft table UM_F_SF_STDNT_EQUTN_VAR.
--
 --V01  SMT-xxxx 06/28/2018,    James Doucette
--                              Converted from SQL Script
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_SF_STDNT_EQUTN_VAR';
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

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_SF_STDNT_EQUTN_VAR';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_SF_STDNT_EQUTN_VAR');

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_SF_STDNT_EQUTN_VAR disable constraint PK_UM_F_SF_STDNT_EQUTN_VAR';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_SF_STDNT_EQUTN_VAR';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_SF_STDNT_EQUTN_VAR';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_SF_STDNT_EQUTN_VAR';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_SF_STDNT_EQUTN_VAR';				
insert into CSMRT_OWNER.UM_F_SF_STDNT_EQUTN_VAR
select
A.INSTITUTION INSTITUTION_CD, A.BILLING_CAREER, A.STRM TERM_CD, A.EMPLID PERSON_ID, A.SRC_SYS_ID, 
nvl(I.INSTITUTION_SID, 2147483646) INSTITUTION_SID, 
nvl(C.ACAD_CAR_SID, 2147483646) BILL_CAR_SID, 
nvl(T.TERM_SID, 2147483646) TERM_SID, 
nvl(P.PERSON_SID, 2147483646) PERSON_SID, 
VARIABLE_CHAR1, VARIABLE_CHAR2, VARIABLE_CHAR3, VARIABLE_CHAR4, VARIABLE_CHAR5, VARIABLE_CHAR6, VARIABLE_CHAR7, VARIABLE_CHAR8, VARIABLE_CHAR9, VARIABLE_CHAR10, 
VARIABLE_FLAG1, VARIABLE_FLAG2, VARIABLE_FLAG3, VARIABLE_FLAG4, VARIABLE_FLAG5, VARIABLE_FLAG6, VARIABLE_FLAG7, VARIABLE_FLAG8, VARIABLE_FLAG9, VARIABLE_FLAG10, 
VARIABLE_NUM1, VARIABLE_NUM2, VARIABLE_NUM3, VARIABLE_NUM4, VARIABLE_NUM5, VARIABLE_NUM6, VARIABLE_NUM7, VARIABLE_NUM8, VARIABLE_NUM9, VARIABLE_NUM10, 
'N', 'S', sysdate, sysdate, 1234
 from CSSTG_OWNER.PS_STDNT_EQUTN_VAR A
 left outer join PS_D_INSTITUTION I
   on A.INSTITUTION = I.INSTITUTION_CD
  and A.SRC_SYS_ID = I.SRC_SYS_ID
 left outer join PS_D_ACAD_CAR C
   on A.INSTITUTION = C.INSTITUTION_CD
  and A.BILLING_CAREER = C.ACAD_CAR_CD
  and A.SRC_SYS_ID = C.SRC_SYS_ID
 left outer join PS_D_TERM T
   on A.INSTITUTION = T.INSTITUTION_CD
  and A.BILLING_CAREER = T.ACAD_CAR_CD
  and A.STRM = T.TERM_CD
  and A.SRC_SYS_ID = T.SRC_SYS_ID
 left outer join PS_D_PERSON P
   on A.EMPLID = P.PERSON_ID
  and A.SRC_SYS_ID = P.SRC_SYS_ID
where A.DATA_ORIGIN <> 'D'
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_SF_STDNT_EQUTN_VAR rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_SF_STDNT_EQUTN_VAR',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_SF_STDNT_EQUTN_VAR',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_SF_STDNT_EQUTN_VAR';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_SF_STDNT_EQUTN_VAR enable constraint PK_UM_F_SF_STDNT_EQUTN_VAR';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_SF_STDNT_EQUTN_VAR');

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

END UM_F_SF_STDNT_EQUTN_VAR_P;
/

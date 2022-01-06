CREATE OR REPLACE PROCEDURE             "UM_F_FA_STDNT_WS_AWARDS_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads table UM_F_FA_STDNT_WS_AWARDS.
--
 --V01  SMT-xxxx 07/06/2018,    James Doucette
--                              Converted from SQL Script
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_FA_STDNT_WS_AWARDS';
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

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_FA_STDNT_WS_AWARDS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_FA_STDNT_WS_AWARDS');

--alter table UM_F_FA_STDNT_WS_AWARDS disable constraint PK_UM_F_FA_STDNT_WS_AWARDS;
strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_FA_STDNT_WS_AWARDS disable constraint PK_UM_F_FA_STDNT_WS_AWARDS';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_FA_STDNT_WS_AWARDS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_FA_STDNT_WS_AWARDS';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_FA_STDNT_WS_AWARDS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_FA_STDNT_WS_AWARDS';				
insert into CSMRT_OWNER.UM_F_FA_STDNT_WS_AWARDS
select 
A.INSTITUTION INSTITUTION_CD, A.ACAD_CAREER ACAD_CAR_CD, A.AID_YEAR, A.EMPLID PERSON_ID, A.ITEM_TYPE, A.SEQNO, A.EFFDT, A.SRC_SYS_ID, 
nvl(I.INSTITUTION_SID, 2147483646) INSTITUTION_SID, 
nvl(C.ACAD_CAR_SID, 2147483646) ACAD_CAR_SID, 
nvl(P.PERSON_SID, 2147483646) PERSON_SID, 
nvl(T.ITEM_TYPE_SID, 2147483646) ITEM_TYPE_SID, 
ACCOUNT, ACTION_DT, AWARD_STATUS, COMMENTS_MSGS, 
COMMUNITY_SERVICE, 
nvl((SELECT min(X.XLATLONGNAME)
       FROM UM_D_XLATITEM X
      WHERE X.FIELDNAME = 'COMMUNITY_SERVICE'
        AND X.FIELDVALUE = COMMUNITY_SERVICE), '-') COMMUNITY_SERVICE_LD,   -- Sept 2016 
EFF_STATUS, EMAILID, EMPLOYER, EMPL_RCD, END_DT, HOURLY_RT, JOBID, JOB_REC_EFFDT, JOB_REC_EFFSEQ, PHONE, SUPERVISOR_NAME, 
UM_EXEMPT, UM_SEC_ACCOUNT, UM_THIRD_ACCOUNT, UM_FOURTH_ACCOUNT, 
WS_PLACEMENT_STAT, 
nvl((SELECT MIN (X.XLATLONGNAME)
       FROM UM_D_XLATITEM X
      WHERE X.FIELDNAME = 'WS_PLACEMENT_STAT'
        AND X.FIELDVALUE = WS_PLACEMENT_STAT), '-') WS_PLACEMENT_STAT_LD,
WS_PLACEMENT_DT, START_DATE, END_DATE,
'N', 'S', sysdate, sysdate, 1234
  from CSSTG_OWNER.PS_UM_STDNT_WS_AWD A
  left outer join PS_D_INSTITUTION I
    on A.INSTITUTION = I.INSTITUTION_CD
   and A.SRC_SYS_ID = I.SRC_SYS_ID
  left outer join PS_D_ACAD_CAR C
    on A.INSTITUTION = C.INSTITUTION_CD
   and A.ACAD_CAREER = C.ACAD_CAR_CD
   and A.SRC_SYS_ID = C.SRC_SYS_ID
  left outer join PS_D_PERSON P
    on A.EMPLID = P.PERSON_ID
   and A.SRC_SYS_ID = P.SRC_SYS_ID
  left outer join PS_D_ITEM_TYPE T
    on A.INSTITUTION = T.SETID
   and A.ITEM_TYPE = T.ITEM_TYPE_ID
   and T.ITEM_CLS_TYPE_ID = 'F'
   and A.SRC_SYS_ID = T.SRC_SYS_ID
 where A.DATA_ORIGIN <> 'D'
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_FA_STDNT_WS_AWARDS rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_FA_STDNT_WS_AWARDS',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_FA_STDNT_WS_AWARDS',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_FA_STDNT_WS_AWARDS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_FA_STDNT_WS_AWARDS enable constraint PK_UM_F_FA_STDNT_WS_AWARDS';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_FA_STDNT_WS_AWARDS');

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

END UM_F_FA_STDNT_WS_AWARDS_P;
/

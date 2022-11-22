DROP PROCEDURE CSMRT_OWNER.AM_PS_STDNT_AWRD_ACTV_S1_P
/

--
-- AM_PS_STDNT_AWRD_ACTV_S1_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."AM_PS_STDNT_AWRD_ACTV_S1_P" IS

------------------------------------------------------------------------
--
-- Pre-Stage procedure for AMSTG_OWNER.PS_STDNT_AWRD_ACTV 
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'AM_PS_STDNT_AWRD_ACTV_S1';
        intProcessSid                   Integer;
        dtProcessStart                  Date            := SYSDATE;
        strMessage01                    Varchar2(4000);
        strMessage02                    Varchar2(512);
        strMessage03                    Varchar2(512)   :='';
        strNewLine                      Varchar2(2)     := chr(13) || chr(10);
        strSqlCommand                   Varchar2(32767) :='';
        strSqlDynamic                   Varchar2(32767) :='';
        strClientInfo                   Varchar2(100);
        strDELETE_FLG                   Varchar2(1);
        intRowCount                     Integer;
        intTotalRowCount                Integer         := 0;
        numSqlCode                      Number;
        strSqlErrm                      Varchar2(4000);
        intTries                        Integer;
        intMaxSCN                       Integer;

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

strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update START_DT on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Reading',
       START_DT = SYSDATE,
       END_DT = NULL
 where TABLE_NAME = 'PS_STDNT_AWRD_ACTV'
;

strSqlCommand := 'commit';
commit;

strSqlCommand   := 'select from AMSTG_OWNER.UM_STAGE_JOBS';
select DELETE_FLG,
       OLD_MAX_SCN
  into strDELETE_FLG,
       intMaxSCN
  from AMSTG_OWNER.UM_STAGE_JOBS 
 where TABLE_NAME = 'PS_STDNT_AWRD_ACTV'
; 

strMessage01    := 'Truncating table AMSTG_OWNER.PS_STDNT_AWRD_ACTV_S1';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table AMSTG_OWNER.PS_STDNT_AWRD_ACTV_S1';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Inserting data into AMSTG_OWNER.PS_STDNT_AWRD_ACTV_S1';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into AMSTG_OWNER.PS_STDNT_AWRD_ACTV_S1';
insert /*+ append parallel(8) enable_parallel_dml */ into AMSTG_OWNER.PS_STDNT_AWRD_ACTV_S1
select /*+ full(S) */
    nvl(trim(EMPLID),'-') EMPLID, 
    nvl(trim(INSTITUTION),'-') INSTITUTION, 
    nvl(trim(AID_YEAR),'-') AID_YEAR, 
    nvl(trim(ITEM_TYPE),'-') ITEM_TYPE, 
    nvl(trim(ACAD_CAREER),'-') ACAD_CAREER, 
    nvl(ACTION_DTTM, to_timestamp(to_date('01-JAN-1900'))) ACTION_DTTM,      
    nvl(trim(DISBURSEMENT_PLAN),'-') DISBURSEMENT_PLAN, 
    nvl(trim(SPLIT_CODE),'-') SPLIT_CODE, 
    nvl(trim(DISBURSEMENT_ID),'-') DISBURSEMENT_ID, 
    nvl(trim(OPRID),'-') OPRID, 
    nvl(trim(AWARD_DISB_ACTION),'-') AWARD_DISB_ACTION, 
    nvl(OFFER_AMOUNT,0) OFFER_AMOUNT, 
    nvl(ACCEPT_AMOUNT,0) ACCEPT_AMOUNT, 
    nvl(AUTHORIZED_AMOUNT,0) AUTHORIZED_AMOUNT, 
    nvl(DISB_AMOUNT,0) DISB_AMOUNT, 
    nvl(trim(CURRENCY_CD),'-') CURRENCY_CD, 
    nvl(trim(BUSINESS_UNIT),'-') BUSINESS_UNIT, 
    nvl(trim(ADJUST_REASON_CD),'-') ADJUST_REASON_CD, 
    nvl(ADJUST_AMOUNT,0) ADJUST_AMOUNT, 
    nvl(trim(LOAN_ADJUST_CD),'-') LOAN_ADJUST_CD, 
    nvl(DISB_TO_DATE,0) DISB_TO_DATE, 
    nvl(AUTH_TO_DATE,0) AUTH_TO_DATE, 
    nvl(trim(PKG_APP_DATA_USED),'-') PKG_APP_DATA_USED,
    ORA_ROWSCN SRC_SCN,
    SYSDATE INSERT_TIME
  from SYSADM.PS_STDNT_AWRD_ACTV@AMSOURCE S 
 where ORA_ROWSCN >= intMaxSCN
   and EMPLID between '00000000' and '99999999'
   and length(EMPLID) = 8
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_STDNT_AWRD_ACTV_S1 rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_STDNT_AWRD_ACTV_S1',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting'
 where TABLE_NAME = 'PS_STDNT_AWRD_ACTV';

strSqlCommand := 'commit';
commit;

If strDELETE_FLG = 'Y' then

strMessage01    := 'Updating DATA_ORIGIN on AMSTG_OWNER.PS_STDNT_AWRD_ACTV';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on AMSTG_OWNER.PS_STDNT_AWRD_ACTV';
update /*+ parallel(8) enable_parallel_dml */ AMSTG_OWNER.PS_STDNT_AWRD_ACTV T
   set T.DATA_ORIGIN = 'D',
       T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, INSTITUTION, AID_YEAR, ITEM_TYPE, ACAD_CAREER, ACTION_DTTM
   from AMSTG_OWNER.PS_STDNT_AWRD_ACTV T2
  minus
 select EMPLID, INSTITUTION, AID_YEAR, ITEM_TYPE, ACAD_CAREER, ACTION_DTTM
   from SYSADM.PS_STDNT_AWRD_ACTV@AMSOURCE S2
   ) S
 where T.EMPLID = S.EMPLID
   and T.INSTITUTION = S.INSTITUTION
   and T.AID_YEAR = S.AID_YEAR
   and T.ITEM_TYPE = S.ITEM_TYPE
   and T.ACAD_CAREER = S.ACAD_CAREER
   and T.ACTION_DTTM = S.ACTION_DTTM 
   and T.SRC_SYS_ID = 'CS90' 
   ) 
; 

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_STDNT_AWRD_ACTV rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_STDNT_AWRD_ACTV',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

End If;

strSqlCommand   := 'update NEW_MAX_SCN on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Staged',
       NEW_MAX_SCN = (select max(SRC_SCN) from AMSTG_OWNER.PS_STDNT_AWRD_ACTV_S1),
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_STDNT_AWRD_ACTV'
;

strSqlCommand := 'commit';
commit;

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

strMessage01    := strProcessName || ' is complete.';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

EXCEPTION
   WHEN OTHERS
   THEN
      COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_EXCEPTION (
         i_SqlCommand   => strSqlCommand,
         i_SqlCode      => SQLCODE,
         i_SqlErrm      => SQLERRM);

END AM_PS_STDNT_AWRD_ACTV_S1_P;
/

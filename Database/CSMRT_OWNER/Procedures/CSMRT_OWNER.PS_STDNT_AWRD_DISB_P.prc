DROP PROCEDURE CSMRT_OWNER.PS_STDNT_AWRD_DISB_P
/

--
-- PS_STDNT_AWRD_DISB_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_STDNT_AWRD_DISB_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_STDNT_AWRD_DISB from PeopleSoft table PS_STDNT_AWRD_DISB.
--
-- V01  SMT-xxxx 04/11/2017,    Jim Doucette
--                              Converted from PS_STDNT_AWRD_DISB.SQL
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_STDNT_AWRD_DISB';
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
 where TABLE_NAME = 'PS_STDNT_AWRD_DISB'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_STDNT_AWRD_DISB@SASOURCE S)
 where TABLE_NAME = 'PS_STDNT_AWRD_DISB'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_STDNT_AWRD_DISB';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_STDNT_AWRD_DISB';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_STDNT_AWRD_DISB T
using (select /*+ full(S) */
    nvl(trim(EMPLID),'-') EMPLID, 
    nvl(trim(INSTITUTION),'-') INSTITUTION, 
    nvl(trim(AID_YEAR),'-') AID_YEAR, 
    nvl(trim(ITEM_TYPE),'-') ITEM_TYPE, 
    nvl(trim(ACAD_CAREER),'-') ACAD_CAREER, 
    nvl(trim(DISBURSEMENT_ID),'-') DISBURSEMENT_ID, 
    nvl(OFFER_BALANCE,0) OFFER_BALANCE, 
    nvl(ACCEPT_BALANCE,0) ACCEPT_BALANCE, 
    nvl(AUTHORIZED_BALANCE,0) AUTHORIZED_BALANCE, 
    nvl(DISBURSED_BALANCE,0) DISBURSED_BALANCE, 
    nvl(NET_DISB_BALANCE,0) NET_DISB_BALANCE, 
    nvl(trim(CURRENCY_CD),'-') CURRENCY_CD, 
    nvl(trim(BUSINESS_UNIT),'-') BUSINESS_UNIT, 
    nvl(trim(AGGREGATE_AREA),'-') AGGREGATE_AREA, 
    nvl(trim(AGGREGATE_LEVEL),'-') AGGREGATE_LEVEL, 
    nvl(trim(CPS_SCHOOL_CODE),'-') CPS_SCHOOL_CODE, 
    nvl(OFFER_BAL_LN_FEE,0) OFFER_BAL_LN_FEE, 
    nvl(ACCEPT_BAL_LN_FEE,0) ACCEPT_BAL_LN_FEE, 
    nvl(OFFER_BAL_REBATE,0) OFFER_BAL_REBATE, 
    nvl(ACCEPT_BAL_REBATE,0) ACCEPT_BAL_REBATE, 
    nvl(SFA_AGGR_BALANCE,0) SFA_AGGR_BALANCE, 
    nvl(trim(STRM),'-') STRM
  from SYSADM.PS_STDNT_AWRD_DISB@SASOURCE S 
 where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_STDNT_AWRD_DISB')
   and EMPLID between '00000000' and '99999999'
   and length(EMPLID) = 8 ) S
 on ( 
    T.EMPLID = S.EMPLID and 
    T.INSTITUTION = S.INSTITUTION and 
    T.AID_YEAR = S.AID_YEAR and 
    T.ITEM_TYPE = S.ITEM_TYPE and 
    T.ACAD_CAREER = S.ACAD_CAREER and 
    T.DISBURSEMENT_ID = S.DISBURSEMENT_ID and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.OFFER_BALANCE = S.OFFER_BALANCE,
    T.ACCEPT_BALANCE = S.ACCEPT_BALANCE,
    T.AUTHORIZED_BALANCE = S.AUTHORIZED_BALANCE,
    T.DISBURSED_BALANCE = S.DISBURSED_BALANCE,
    T.NET_DISB_BALANCE = S.NET_DISB_BALANCE,
    T.CURRENCY_CD = S.CURRENCY_CD,
    T.BUSINESS_UNIT = S.BUSINESS_UNIT,
    T.AGGREGATE_AREA = S.AGGREGATE_AREA,
    T.AGGREGATE_LEVEL = S.AGGREGATE_LEVEL,
    T.CPS_SCHOOL_CODE = S.CPS_SCHOOL_CODE,
    T.OFFER_BAL_LN_FEE = S.OFFER_BAL_LN_FEE,
    T.ACCEPT_BAL_LN_FEE = S.ACCEPT_BAL_LN_FEE,
    T.OFFER_BAL_REBATE = S.OFFER_BAL_REBATE,
    T.ACCEPT_BAL_REBATE = S.ACCEPT_BAL_REBATE,
    T.SFA_AGGR_BALANCE = S.SFA_AGGR_BALANCE,
    T.STRM = S.STRM,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.OFFER_BALANCE <> S.OFFER_BALANCE or 
    T.ACCEPT_BALANCE <> S.ACCEPT_BALANCE or 
    T.AUTHORIZED_BALANCE <> S.AUTHORIZED_BALANCE or 
    T.DISBURSED_BALANCE <> S.DISBURSED_BALANCE or 
    T.NET_DISB_BALANCE <> S.NET_DISB_BALANCE or 
    T.CURRENCY_CD <> S.CURRENCY_CD or 
    T.BUSINESS_UNIT <> S.BUSINESS_UNIT or 
    T.AGGREGATE_AREA <> S.AGGREGATE_AREA or 
    T.AGGREGATE_LEVEL <> S.AGGREGATE_LEVEL or 
    T.CPS_SCHOOL_CODE <> S.CPS_SCHOOL_CODE or 
    T.OFFER_BAL_LN_FEE <> S.OFFER_BAL_LN_FEE or 
    T.ACCEPT_BAL_LN_FEE <> S.ACCEPT_BAL_LN_FEE or 
    T.OFFER_BAL_REBATE <> S.OFFER_BAL_REBATE or 
    T.ACCEPT_BAL_REBATE <> S.ACCEPT_BAL_REBATE or 
    T.SFA_AGGR_BALANCE <> S.SFA_AGGR_BALANCE or 
    T.STRM <> S.STRM or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.EMPLID, 
    T.INSTITUTION,
    T.AID_YEAR, 
    T.ITEM_TYPE,
    T.ACAD_CAREER,
    T.DISBURSEMENT_ID,
    T.SRC_SYS_ID, 
    T.OFFER_BALANCE,
    T.ACCEPT_BALANCE, 
    T.AUTHORIZED_BALANCE, 
    T.DISBURSED_BALANCE,
    T.NET_DISB_BALANCE, 
    T.CURRENCY_CD,
    T.BUSINESS_UNIT,
    T.AGGREGATE_AREA, 
    T.AGGREGATE_LEVEL,
    T.CPS_SCHOOL_CODE,
    T.OFFER_BAL_LN_FEE, 
    T.ACCEPT_BAL_LN_FEE,
    T.OFFER_BAL_REBATE, 
    T.ACCEPT_BAL_REBATE,
    T.SFA_AGGR_BALANCE, 
    T.STRM, 
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
) 
values (
    S.EMPLID, 
    S.INSTITUTION,
    S.AID_YEAR, 
    S.ITEM_TYPE,
    S.ACAD_CAREER,
    S.DISBURSEMENT_ID,
    'CS90', 
    S.OFFER_BALANCE,
    S.ACCEPT_BALANCE, 
    S.AUTHORIZED_BALANCE, 
    S.DISBURSED_BALANCE,
    S.NET_DISB_BALANCE, 
    S.CURRENCY_CD,
    S.BUSINESS_UNIT,
    S.AGGREGATE_AREA, 
    S.AGGREGATE_LEVEL,
    S.CPS_SCHOOL_CODE,
    S.OFFER_BAL_LN_FEE, 
    S.ACCEPT_BAL_LN_FEE,
    S.OFFER_BAL_REBATE, 
    S.ACCEPT_BAL_REBATE,
    S.SFA_AGGR_BALANCE, 
    S.STRM, 
    'N',
    'S',
    sysdate,
    sysdate,
    1234);

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_STDNT_AWRD_DISB rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_STDNT_AWRD_DISB',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_STDNT_AWRD_DISB';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_STDNT_AWRD_DISB';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_STDNT_AWRD_DISB';
update CSSTG_OWNER.PS_STDNT_AWRD_DISB T
   set T.DATA_ORIGIN = 'D',
          T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, INSTITUTION, AID_YEAR, ITEM_TYPE, ACAD_CAREER, DISBURSEMENT_ID
   from CSSTG_OWNER.PS_STDNT_AWRD_DISB T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_STDNT_AWRD_DISB') = 'Y'
  minus
 select EMPLID, INSTITUTION, AID_YEAR, ITEM_TYPE, ACAD_CAREER, DISBURSEMENT_ID
   from SYSADM.PS_STDNT_AWRD_DISB@SASOURCE S2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_STDNT_AWRD_DISB') = 'Y'
   ) S
 where T.EMPLID = S.EMPLID
   and T.INSTITUTION = S.INSTITUTION
   and T.AID_YEAR = S.AID_YEAR
   and T.ITEM_TYPE = S.ITEM_TYPE
   and T.ACAD_CAREER = S.ACAD_CAREER
   and T.DISBURSEMENT_ID = S.DISBURSEMENT_ID
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_STDNT_AWRD_DISB rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_STDNT_AWRD_DISB',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_STDNT_AWRD_DISB'
;

strSqlCommand := 'commit';
commit;


strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

strMessage01    := strProcessName || ' is complete.';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);


EXCEPTION
    WHEN OTHERS THEN
        numSqlCode := SQLCODE;
        strSqlErrm := SQLERRM;

        ROLLBACK;
  
        strMessage01 := 'Error code: ' || TO_CHAR(SQLCODE) || ' Error Message: ' || SQLERRM;
        strMessage02 := TO_CHAR(SQLCODE);
  
        COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_FAILURE
                       (i_SqlCommand    => strSqlCommand,
                        i_ErrorText     => strMessage01,
                        i_ErrorCode     => strMessage02,
                        i_ErrorMessage  => strSqlErrm
                       );
               
        strMessage01 := 'Error...'
                        || strNewLine   || 'SQL Command:   ' || strSqlCommand
                        || strNewLine   || 'Error code:    ' || numSqlCode
                        || strNewLine   || 'Error Message: ' || strSqlErrm;

        COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
        RAISE_APPLICATION_ERROR( -20001, strMessage01);

END PS_STDNT_AWRD_DISB_P;
/

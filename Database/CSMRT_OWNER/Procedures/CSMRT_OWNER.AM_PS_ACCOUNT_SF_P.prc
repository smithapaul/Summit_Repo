DROP PROCEDURE CSMRT_OWNER.AM_PS_ACCOUNT_SF_P
/

--
-- AM_PS_ACCOUNT_SF_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER.AM_PS_ACCOUNT_SF_P IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_ACCOUNT_SF from PeopleSoft table PS_ACCOUNT_SF.
--
-- V01  SMT-xxxx 03/28/2017,    George Adams
--                              Converted from PS_ACCOUNT_SF.SQL
--VXX    07/06/2021,            Kieu ,Srikanth - Added EMPLID or COMMON_ID additional filter logic 
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'AM_PS_ACCOUNT_SF';
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

strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);


strSqlCommand   := 'update START_DT on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Reading',
       START_DT = sysdate,
       END_DT = NULL
 where TABLE_NAME = 'PS_ACCOUNT_SF'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_ACCOUNT_SF@AMSOURCE S)
 where TABLE_NAME = 'PS_ACCOUNT_SF'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into AMSTG_OWNER.PS_ACCOUNT_SF';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into AMSTG_OWNER.PS_ACCOUNT_SF';
merge /*+ use_hash(S,T) */ into AMSTG_OWNER.PS_ACCOUNT_SF T 
    using (select /*+ full(S) */
    nvl(trim(BUSINESS_UNIT),'-') BUSINESS_UNIT, 
    nvl(trim(EMPLID),'-') EMPLID, 
    nvl(trim(ACCOUNT_NBR),'-') ACCOUNT_NBR, 
    nvl(trim(ACCOUNT_TERM),'-') ACCOUNT_TERM, 
    nvl(trim(ACCOUNT_TYPE_SF),'-') ACCOUNT_TYPE_SF, 
    nvl(trim(ACCT_STATUS),'-') ACCT_STATUS, 
    to_date(to_char(case when OPEN_DT < '01-JAN-1800' then NULL 
                    else OPEN_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') OPEN_DT, 
    to_date(to_char(case when CLOSE_DT < '01-JAN-1800' then NULL 
                    else CLOSE_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') CLOSE_DT,
    nvl(trim(CONTRACT_NUM),'-') CONTRACT_NUM, 
    nvl(ACCOUNT_BALANCE,0) ACCOUNT_BALANCE, 
    nvl(trim(ACAD_YEAR),'-') ACAD_YEAR, 
    to_date(to_char(case when LAST_AGING_DT < '01-JAN-1800' then NULL 
                    else LAST_AGING_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') LAST_AGING_DT, 
    to_date(to_char(case when LAST_ACCT_DT_AGED < '01-JAN-1800' then NULL 
                    else LAST_ACCT_DT_AGED end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') LAST_ACCT_DT_AGED, 
    nvl(trim(BILL_REQ_ID),'-') BILL_REQ_ID, 
    nvl(trim(OVERR_BILL_REQ_ID),'-') OVERR_BILL_REQ_ID, 
    nvl(trim(INCLUDE_IN_BALANCE),'-') INCLUDE_IN_BALANCE, 
    nvl(trim(INCLUDE_BILLING),'-') INCLUDE_BILLING, 
    nvl(trim(INCLUDE_TRANSFER),'-') INCLUDE_TRANSFER, 
    nvl(trim(INCLUDE_PREPAY),'-') INCLUDE_PREPAY
  from SYSADM.PS_ACCOUNT_SF@AMSOURCE S
 where ORA_ROWSCN > (select OLD_MAX_SCN from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ACCOUNT_SF')
   and EMPLID between '00000000' and '99999999'
   and length(EMPLID) = 8 ) S 
 on ( 
    T.BUSINESS_UNIT = S.BUSINESS_UNIT and 
    T.EMPLID = S.EMPLID and 
    T.ACCOUNT_NBR = S.ACCOUNT_NBR and 
    T.ACCOUNT_TERM = S.ACCOUNT_TERM and 
    T.SRC_SYS_ID = 'CS90')
    when matched then update set
    T.ACCOUNT_TYPE_SF = S.ACCOUNT_TYPE_SF,
    T.ACCT_STATUS = S.ACCT_STATUS,
    T.OPEN_DT = S.OPEN_DT,
    T.CLOSE_DT = S.CLOSE_DT,
    T.CONTRACT_NUM = S.CONTRACT_NUM,
    T.ACCOUNT_BALANCE = S.ACCOUNT_BALANCE,
    T.ACAD_YEAR = S.ACAD_YEAR,
    T.LAST_AGING_DT = S.LAST_AGING_DT,
    T.LAST_ACCT_DT_AGED = S.LAST_ACCT_DT_AGED,
    T.BILL_REQ_ID = S.BILL_REQ_ID,
    T.OVERR_BILL_REQ_ID = S.OVERR_BILL_REQ_ID,
    T.INCLUDE_IN_BALANCE = S.INCLUDE_IN_BALANCE,
    T.INCLUDE_BILLING = S.INCLUDE_BILLING,
    T.INCLUDE_TRANSFER = S.INCLUDE_TRANSFER,
    T.INCLUDE_PREPAY = S.INCLUDE_PREPAY,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.ACCOUNT_TYPE_SF <> S.ACCOUNT_TYPE_SF or 
    T.ACCT_STATUS <> S.ACCT_STATUS or 
    T.OPEN_DT <> S.OPEN_DT or 
    nvl(trim(T.CLOSE_DT),0) <> nvl(trim(S.CLOSE_DT),0) or 
    T.CONTRACT_NUM <> S.CONTRACT_NUM or 
    T.ACCOUNT_BALANCE <> S.ACCOUNT_BALANCE or 
    T.ACAD_YEAR <> S.ACAD_YEAR or 
    nvl(trim(T.LAST_AGING_DT),0) <> nvl(trim(S.LAST_AGING_DT),0) or 
    nvl(trim(T.LAST_ACCT_DT_AGED),0) <> nvl(trim(S.LAST_ACCT_DT_AGED),0) or 
    T.BILL_REQ_ID <> S.BILL_REQ_ID or 
    T.OVERR_BILL_REQ_ID <> S.OVERR_BILL_REQ_ID or 
    T.INCLUDE_IN_BALANCE <> S.INCLUDE_IN_BALANCE or 
    T.INCLUDE_BILLING <> S.INCLUDE_BILLING or 
    T.INCLUDE_TRANSFER <> S.INCLUDE_TRANSFER or 
    T.INCLUDE_PREPAY <> S.INCLUDE_PREPAY or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.BUSINESS_UNIT,
    T.EMPLID, 
    T.ACCOUNT_NBR,
    T.ACCOUNT_TERM, 
    T.SRC_SYS_ID, 
    T.ACCOUNT_TYPE_SF,
    T.ACCT_STATUS,
    T.OPEN_DT,
    T.CLOSE_DT, 
    T.CONTRACT_NUM, 
    T.ACCOUNT_BALANCE,
    T.ACAD_YEAR,
    T.LAST_AGING_DT,
    T.LAST_ACCT_DT_AGED,
    T.BILL_REQ_ID,
    T.OVERR_BILL_REQ_ID,
    T.INCLUDE_IN_BALANCE, 
    T.INCLUDE_BILLING,
    T.INCLUDE_TRANSFER, 
    T.INCLUDE_PREPAY, 
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
) 
values (
    S.BUSINESS_UNIT,
    S.EMPLID, 
    S.ACCOUNT_NBR,
    S.ACCOUNT_TERM, 
    'CS90', 
    S.ACCOUNT_TYPE_SF,
    S.ACCT_STATUS,
    S.OPEN_DT,
    S.CLOSE_DT, 
    S.CONTRACT_NUM, 
    S.ACCOUNT_BALANCE,
    S.ACAD_YEAR,
    S.LAST_AGING_DT,
    S.LAST_ACCT_DT_AGED,
    S.BILL_REQ_ID,
    S.OVERR_BILL_REQ_ID,
    S.INCLUDE_IN_BALANCE, 
    S.INCLUDE_BILLING,
    S.INCLUDE_TRANSFER, 
    S.INCLUDE_PREPAY, 
    'N',
    'S',
    sysdate,
    sysdate,
    1234);

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_ACCOUNT_SF rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_ACCOUNT_SF',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_ACCOUNT_SF';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on AMSTG_OWNER.PS_ACCOUNT_SF';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on AMSTG_OWNER.PS_ACCOUNT_SF';
update AMSTG_OWNER.PS_ACCOUNT_SF T
   set T.DATA_ORIGIN = 'D',
       T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select BUSINESS_UNIT, EMPLID, ACCOUNT_NBR, ACCOUNT_TERM
   from AMSTG_OWNER.PS_ACCOUNT_SF T2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ACCOUNT_SF') = 'Y'
  minus
 select BUSINESS_UNIT, EMPLID, ACCOUNT_NBR, ACCOUNT_TERM
   from SYSADM.PS_ACCOUNT_SF@AMSOURCE S2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ACCOUNT_SF') = 'Y'
   ) S
 where T.BUSINESS_UNIT = S.BUSINESS_UNIT
   and T.EMPLID = S.EMPLID
   and T.ACCOUNT_NBR = S.ACCOUNT_NBR
   and T.ACCOUNT_TERM = S.ACCOUNT_TERM
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_ACCOUNT_SF rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_ACCOUNT_SF',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on AMSTG_OWNER.UM_STAGE_JOBS';

update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_ACCOUNT_SF'
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

END AM_PS_ACCOUNT_SF_P;
/

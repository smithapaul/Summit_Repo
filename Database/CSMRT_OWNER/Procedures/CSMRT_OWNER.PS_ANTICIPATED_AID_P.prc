CREATE OR REPLACE PROCEDURE             "PS_ANTICIPATED_AID_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_ANTICIPATED_AID from PeopleSoft table PS_ANTICIPATED_AID.
--
-- V01  SMT-xxxx 04/04/2017,    Jim Doucette
--                              Converted from PS_ANTICIPATED_AID.SQL
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_ANTICIPATED_AID';
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
 where TABLE_NAME = 'PS_ANTICIPATED_AID'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_ANTICIPATED_AID@SASOURCE S)
 where TABLE_NAME = 'PS_ANTICIPATED_AID'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_ANTICIPATED_AID';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_ANTICIPATED_AID';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_ANTICIPATED_AID T
using (select /*+ full(S) */
    nvl(trim(EMPLID),'-') EMPLID, 
    nvl(trim(INSTITUTION),'-') INSTITUTION, 
    nvl(trim(AID_YEAR),'-') AID_YEAR, 
    nvl(trim(ITEM_TYPE),'-') ITEM_TYPE, 
    nvl(trim(ACAD_CAREER),'-') ACAD_CAREER, 
    nvl(trim(DISBURSEMENT_PLAN),'-') DISBURSEMENT_PLAN, 
    nvl(trim(DISBURSEMENT_ID),'-') DISBURSEMENT_ID, 
--    AS_OF_DTTM AS_OF_DTTM,
    nvl(AS_OF_DTTM,to_date('01-JAN-1900')) AS_OF_DTTM,     -- May 2018 
    STRM STRM,
    NET_AWARD_AMT NET_AWARD_AMT,
    to_date(to_char(case when DISB_APPLY_DT < '01-JAN-1800' then NULL 
     else DISB_APPLY_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') DISB_APPLY_DT, 
    to_date(to_char(case when DISB_EXPIRE_DT < '01-JAN-1800' then NULL 
                    else DISB_EXPIRE_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') DISB_EXPIRE_DT,
    CURRENCY_CD CURRENCY_CD
  from SYSADM.PS_ANTICIPATED_AID@SASOURCE S 
 where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ANTICIPATED_AID')
   and EMPLID between '00000000' and '99999999'
   and length(EMPLID) = 8 ) S
on ( 
    T.EMPLID = S.EMPLID and 
    T.INSTITUTION = S.INSTITUTION and 
    T.AID_YEAR = S.AID_YEAR and 
    T.ITEM_TYPE = S.ITEM_TYPE and 
    T.ACAD_CAREER = S.ACAD_CAREER and 
    T.DISBURSEMENT_PLAN = S.DISBURSEMENT_PLAN and 
    T.DISBURSEMENT_ID = S.DISBURSEMENT_ID and 
    T.AS_OF_DTTM = S.AS_OF_DTTM and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.STRM = S.STRM,
    T.NET_AWARD_AMT = S.NET_AWARD_AMT,
    T.DISB_APPLY_DT = S.DISB_APPLY_DT,
    T.DISB_EXPIRE_DT = S.DISB_EXPIRE_DT,
    T.CURRENCY_CD = S.CURRENCY_CD,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    nvl(trim(T.STRM),0) <> nvl(trim(S.STRM),0) or 
    nvl(trim(T.NET_AWARD_AMT),0) <> nvl(trim(S.NET_AWARD_AMT),0) or 
    nvl(trim(T.DISB_APPLY_DT),0) <> nvl(trim(S.DISB_APPLY_DT),0) or 
    nvl(trim(T.DISB_EXPIRE_DT),0) <> nvl(trim(S.DISB_EXPIRE_DT),0) or 
    nvl(trim(T.CURRENCY_CD),0) <> nvl(trim(S.CURRENCY_CD),0) or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.EMPLID, 
    T.INSTITUTION,
    T.AID_YEAR, 
    T.ITEM_TYPE,
    T.ACAD_CAREER,
    T.DISBURSEMENT_PLAN,
    T.DISBURSEMENT_ID,
    T.AS_OF_DTTM, 
    T.SRC_SYS_ID, 
    T.STRM, 
    T.NET_AWARD_AMT,
    T.DISB_APPLY_DT,
    T.DISB_EXPIRE_DT, 
    T.CURRENCY_CD,
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
    S.DISBURSEMENT_PLAN,
    S.DISBURSEMENT_ID,
    S.AS_OF_DTTM, 
    'CS90', 
    S.STRM, 
    S.NET_AWARD_AMT,
    S.DISB_APPLY_DT,
    S.DISB_EXPIRE_DT, 
    S.CURRENCY_CD,
    'N',
    'S',
    sysdate,
    sysdate,
    1234);


strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_ANTICIPATED_AID rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_ANTICIPATED_AID',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_ANTICIPATED_AID';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_ANTICIPATED_AID';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_ANTICIPATED_AID';
update CSSTG_OWNER.PS_ANTICIPATED_AID T
   set T.DATA_ORIGIN = 'D',
          T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, INSTITUTION, AID_YEAR, ITEM_TYPE, ACAD_CAREER, DISBURSEMENT_PLAN, DISBURSEMENT_ID, AS_OF_DTTM
   from CSSTG_OWNER.PS_ANTICIPATED_AID T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ANTICIPATED_AID') = 'Y'
  minus
 select EMPLID, INSTITUTION, nvl(trim(AID_YEAR),'-') AID_YEAR, nvl(trim(ITEM_TYPE),'-') ITEM_TYPE, nvl(trim(ACAD_CAREER),'-') ACAD_CAREER, DISBURSEMENT_PLAN, DISBURSEMENT_ID, AS_OF_DTTM
   from SYSADM.PS_ANTICIPATED_AID@SASOURCE S2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ANTICIPATED_AID') = 'Y'
   ) S
 where T.EMPLID = S.EMPLID
   and T.INSTITUTION = S.INSTITUTION
   and T.AID_YEAR = S.AID_YEAR
   and T.ITEM_TYPE = S.ITEM_TYPE
   and T.ACAD_CAREER = S.ACAD_CAREER
   and T.DISBURSEMENT_PLAN = S.DISBURSEMENT_PLAN
   and T.DISBURSEMENT_ID = S.DISBURSEMENT_ID
   and T.AS_OF_DTTM = S.AS_OF_DTTM
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_ANTICIPATED_AID rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_ANTICIPATED_AID',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_ANTICIPATED_AID'
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

END PS_ANTICIPATED_AID_P;
/

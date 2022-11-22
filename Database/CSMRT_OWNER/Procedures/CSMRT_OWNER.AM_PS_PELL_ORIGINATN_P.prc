DROP PROCEDURE CSMRT_OWNER.AM_PS_PELL_ORIGINATN_P
/

--
-- AM_PS_PELL_ORIGINATN_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."AM_PS_PELL_ORIGINATN_P" IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_PELL_ORIGINATN from PeopleSoft table PS_PELL_ORIGINATN.
--
-- V01  SMT-xxxx 04/10/2017,    Jim Doucette
--                              Converted from PS_PELL_ORIGINATN.SQL
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'AM_PS_PELL_ORIGINATN';
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
 where TABLE_NAME = 'PS_PELL_ORIGINATN'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_PELL_ORIGINATN@AMSOURCE S)
 where TABLE_NAME = 'PS_PELL_ORIGINATN'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into AMSTG_OWNER.PS_PELL_ORIGINATN';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into AMSTG_OWNER.PS_PELL_ORIGINATN';
merge /*+ use_hash(S,T) */ into AMSTG_OWNER.PS_PELL_ORIGINATN T
using (select /*+ full(S) */
nvl(trim(EMPLID),'-') EMPLID,
nvl(trim(INSTITUTION),'-') INSTITUTION,
nvl(trim(AID_YEAR),'-') AID_YEAR,
nvl(trim(PELL_ORIG_ID),'-') PELL_ORIG_ID,
TIV_SCHOOL_CODE TIV_SCHOOL_CODE,
PELL_TRANS_STAT PELL_TRANS_STAT,
to_date(to_char(case when PELL_TRANS_STAT_DT < '01-JAN-1800' then NULL else PELL_TRANS_STAT_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') PELL_TRANS_STAT_DT,
UPDATE_PELL_ORG UPDATE_PELL_ORG,
PELL_MANUAL_OVRD PELL_MANUAL_OVRD,
PELL_ORIG_STATUS PELL_ORIG_STATUS,
to_date(to_char(case when PELL_ORIG_STAT_DT < '01-JAN-1800' then NULL else PELL_ORIG_STAT_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') PELL_ORIG_STAT_DT,
PELL_MRR_STATUS PELL_MRR_STATUS,
to_date(to_char(case when PELL_MRR_STAT_DT < '01-JAN-1800' then NULL else PELL_MRR_STAT_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') PELL_MRR_STAT_DT
  from SYSADM.PS_PELL_ORIGINATN@AMSOURCE S
 where ORA_ROWSCN > (select OLD_MAX_SCN from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_PELL_ORIGINATN')
   and EMPLID between '00000000' and '99999999'
   and length(EMPLID) = 8 ) S
   on (
T.EMPLID = S.EMPLID and
T.INSTITUTION = S.INSTITUTION and
T.AID_YEAR = S.AID_YEAR and
T.PELL_ORIG_ID = S.PELL_ORIG_ID and
T.SRC_SYS_ID = 'CS90')
when matched then update set
T.TIV_SCHOOL_CODE = S.TIV_SCHOOL_CODE,
T.PELL_TRANS_STAT = S.PELL_TRANS_STAT,
T.PELL_TRANS_STAT_DT = S.PELL_TRANS_STAT_DT,
T.UPDATE_PELL_ORG = S.UPDATE_PELL_ORG,
T.PELL_MANUAL_OVRD = S.PELL_MANUAL_OVRD,
T.PELL_ORIG_STATUS = S.PELL_ORIG_STATUS,
T.PELL_ORIG_STAT_DT = S.PELL_ORIG_STAT_DT,
T.PELL_MRR_STATUS = S.PELL_MRR_STATUS,
T.PELL_MRR_STAT_DT = S.PELL_MRR_STAT_DT,
T.DATA_ORIGIN = 'S',
T.LASTUPD_EW_DTTM = sysdate,
T.BATCH_SID   = 1234
where
nvl(trim(T.TIV_SCHOOL_CODE),0) <> nvl(trim(S.TIV_SCHOOL_CODE),0) or
nvl(trim(T.PELL_TRANS_STAT),0) <> nvl(trim(S.PELL_TRANS_STAT),0) or
nvl(trim(T.PELL_TRANS_STAT_DT),0) <> nvl(trim(S.PELL_TRANS_STAT_DT),0) or
nvl(trim(T.UPDATE_PELL_ORG),0) <> nvl(trim(S.UPDATE_PELL_ORG),0) or
nvl(trim(T.PELL_MANUAL_OVRD),0) <> nvl(trim(S.PELL_MANUAL_OVRD),0) or
nvl(trim(T.PELL_ORIG_STATUS),0) <> nvl(trim(S.PELL_ORIG_STATUS),0) or
nvl(trim(T.PELL_ORIG_STAT_DT),0) <> nvl(trim(S.PELL_ORIG_STAT_DT),0) or
nvl(trim(T.PELL_MRR_STATUS),0) <> nvl(trim(S.PELL_MRR_STATUS),0) or
nvl(trim(T.PELL_MRR_STAT_DT),0) <> nvl(trim(S.PELL_MRR_STAT_DT),0) or
T.DATA_ORIGIN = 'D'
when not matched then
insert (
T.EMPLID,
T.INSTITUTION,
T.AID_YEAR,
T.PELL_ORIG_ID,
T.SRC_SYS_ID,
T.TIV_SCHOOL_CODE,
T.PELL_TRANS_STAT,
T.PELL_TRANS_STAT_DT,
T.UPDATE_PELL_ORG,
T.PELL_MANUAL_OVRD,
T.PELL_ORIG_STATUS,
T.PELL_ORIG_STAT_DT,
T.PELL_MRR_STATUS,
T.PELL_MRR_STAT_DT,
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
S.PELL_ORIG_ID,
'CS90',
S.TIV_SCHOOL_CODE,
S.PELL_TRANS_STAT,
S.PELL_TRANS_STAT_DT,
S.UPDATE_PELL_ORG,
S.PELL_MANUAL_OVRD,
S.PELL_ORIG_STATUS,
S.PELL_ORIG_STAT_DT,
S.PELL_MRR_STATUS,
S.PELL_MRR_STAT_DT,
'N',
'S',
sysdate,
sysdate,
1234);

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_PELL_ORIGINATN rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_PELL_ORIGINATN',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_PELL_ORIGINATN';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on AMSTG_OWNER.PS_PELL_ORIGINATN';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on AMSTG_OWNER.PS_PELL_ORIGINATN';
update AMSTG_OWNER.PS_PELL_ORIGINATN T
   set T.DATA_ORIGIN = 'D',
          T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, INSTITUTION, AID_YEAR, PELL_ORIG_ID
   from AMSTG_OWNER.PS_PELL_ORIGINATN T2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_PELL_ORIGINATN') = 'Y'
  minus
 select EMPLID, INSTITUTION, AID_YEAR, PELL_ORIG_ID
   from SYSADM.PS_PELL_ORIGINATN@AMSOURCE S2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_PELL_ORIGINATN') = 'Y'
   ) S
 where T.EMPLID = S.EMPLID
   and T.INSTITUTION = S.INSTITUTION
   and T.AID_YEAR = S.AID_YEAR
   and T.PELL_ORIG_ID = S.PELL_ORIG_ID
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_PELL_ORIGINATN rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_PELL_ORIGINATN',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on AMSTG_OWNER.UM_STAGE_JOBS';

update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_PELL_ORIGINATN'
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

END AM_PS_PELL_ORIGINATN_P;
/

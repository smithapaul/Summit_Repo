DROP PROCEDURE CSMRT_OWNER.PS_LOAN_ORIG_ACTN_P
/

--
-- PS_LOAN_ORIG_ACTN_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_LOAN_ORIG_ACTN_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_LOAN_ORIG_ACTN from PeopleSoft table PS_LOAN_ORIG_ACTN.
--
-- V01  SMT-xxxx 04/04/2017,    Jim Doucette
--                              Converted from PS_LOAN_ORIG_ACTN.SQL
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_LOAN_ORIG_ACTN';
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
 where TABLE_NAME = 'PS_LOAN_ORIG_ACTN'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_LOAN_ORIG_ACTN@SASOURCE S)
 where TABLE_NAME = 'PS_LOAN_ORIG_ACTN'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_LOAN_ORIG_ACTN';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_LOAN_ORIG_ACTN';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_LOAN_ORIG_ACTN T
using (select /*+ full(S) */
nvl(trim(EMPLID),'-') EMPLID,
nvl(trim(INSTITUTION),'-') INSTITUTION,
nvl(trim(AID_YEAR),'-') AID_YEAR,
nvl(trim(ACAD_CAREER),'-') ACAD_CAREER,
nvl(trim(LOAN_TYPE),'-') LOAN_TYPE,
nvl(LN_APPL_SEQ,0) LN_APPL_SEQ,
nvl(trim(ITEM_TYPE),'-') ITEM_TYPE,
nvl(LN_ORIG_ACTN_SEQ,0) LN_ORIG_ACTN_SEQ,
nvl(trim(LN_ACTION_TYPE),'-') LN_ACTION_TYPE,
nvl(trim(LN_ACTION_CD),'-') LN_ACTION_CD,
to_date(to_char(case when LN_ACTION_DT < '01-JAN-1800' then NULL else LN_ACTION_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') LN_ACTION_DT,
nvl(trim(TRNSFR_BATCH),'-') TRNSFR_BATCH,
nvl(trim(LN_ACTION_STATUS),'-') LN_ACTION_STATUS,
to_date(to_char(case when LN_ACTNSTAT_DT < '01-JAN-1800' then NULL else LN_ACTNSTAT_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') LN_ACTNSTAT_DT,
nvl(trim(DL_LN_APPL_ID_STAT),'-') DL_LN_APPL_ID_STAT,
nvl(trim(OPRID),'-') OPRID,
nvl(PROCESS_INSTANCE,0) PROCESS_INSTANCE,
nvl(trim(CL_RECIP_NAME),'-') CL_RECIP_NAME,
nvl(trim(CL_RECIP_ID_V3),'-') CL_RECIP_ID_V3,
nvl(trim(CL_PHASE_CD),'-') CL_PHASE_CD,
nvl(trim(SFA_CR_DOCUMENT_ID),'-') SFA_CR_DOCUMENT_ID,
nvl(trim(SFA_CR_LNDR_ST_CD),'-') SFA_CR_LNDR_ST_CD,
nvl(trim(SFA_CR_GUAR_ST_CD),'-') SFA_CR_GUAR_ST_CD,
nvl(trim(SFA_CR_PNT_STAT_CD),'-') SFA_CR_PNT_STAT_CD,
nvl(trim(SFA_CR_CRD_STAT_CD),'-') SFA_CR_CRD_STAT_CD,
nvl(trim(SFA_CR_RECIP_ID),'-') SFA_CR_RECIP_ID
  from SYSADM.PS_LOAN_ORIG_ACTN@SASOURCE S
 where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_LOAN_ORIG_ACTN')
   and EMPLID between '00000000' and '99999999'
   and length(EMPLID) = 8 ) S
   on (
T.EMPLID = S.EMPLID and
T.INSTITUTION = S.INSTITUTION and
T.AID_YEAR = S.AID_YEAR and
T.ACAD_CAREER = S.ACAD_CAREER and
T.LOAN_TYPE = S.LOAN_TYPE and
T.LN_APPL_SEQ = S.LN_APPL_SEQ and
T.ITEM_TYPE = S.ITEM_TYPE and
T.LN_ORIG_ACTN_SEQ = S.LN_ORIG_ACTN_SEQ and
T.SRC_SYS_ID = 'CS90')
when matched then update set
T.LN_ACTION_TYPE = S.LN_ACTION_TYPE,
T.LN_ACTION_CD = S.LN_ACTION_CD,
T.LN_ACTION_DT = S.LN_ACTION_DT,
T.TRNSFR_BATCH = S.TRNSFR_BATCH,
T.LN_ACTION_STATUS = S.LN_ACTION_STATUS,
T.LN_ACTNSTAT_DT = S.LN_ACTNSTAT_DT,
T.DL_LN_APPL_ID_STAT = S.DL_LN_APPL_ID_STAT,
T.OPRID = S.OPRID,
T.PROCESS_INSTANCE = S.PROCESS_INSTANCE,
T.CL_RECIP_NAME = S.CL_RECIP_NAME,
T.CL_RECIP_ID_V3 = S.CL_RECIP_ID_V3,
T.CL_PHASE_CD = S.CL_PHASE_CD,
T.SFA_CR_DOCUMENT_ID = S.SFA_CR_DOCUMENT_ID,
T.SFA_CR_LNDR_ST_CD = S.SFA_CR_LNDR_ST_CD,
T.SFA_CR_GUAR_ST_CD = S.SFA_CR_GUAR_ST_CD,
T.SFA_CR_PNT_STAT_CD = S.SFA_CR_PNT_STAT_CD,
T.SFA_CR_CRD_STAT_CD = S.SFA_CR_CRD_STAT_CD,
T.SFA_CR_RECIP_ID = S.SFA_CR_RECIP_ID,
T.DATA_ORIGIN = 'S',
T.LASTUPD_EW_DTTM = sysdate,
T.BATCH_SID   = 1234
where
T.LN_ACTION_TYPE <> S.LN_ACTION_TYPE or
T.LN_ACTION_CD <> S.LN_ACTION_CD or
nvl(trim(T.LN_ACTION_DT),0) <> nvl(trim(S.LN_ACTION_DT),0) or
T.TRNSFR_BATCH <> S.TRNSFR_BATCH or
T.LN_ACTION_STATUS <> S.LN_ACTION_STATUS or
nvl(trim(T.LN_ACTNSTAT_DT),0) <> nvl(trim(S.LN_ACTNSTAT_DT),0) or
T.DL_LN_APPL_ID_STAT <> S.DL_LN_APPL_ID_STAT or
T.OPRID <> S.OPRID or
T.PROCESS_INSTANCE <> S.PROCESS_INSTANCE or
T.CL_RECIP_NAME <> S.CL_RECIP_NAME or
T.CL_RECIP_ID_V3 <> S.CL_RECIP_ID_V3 or
T.CL_PHASE_CD <> S.CL_PHASE_CD or
T.SFA_CR_DOCUMENT_ID <> S.SFA_CR_DOCUMENT_ID or
T.SFA_CR_LNDR_ST_CD <> S.SFA_CR_LNDR_ST_CD or
T.SFA_CR_GUAR_ST_CD <> S.SFA_CR_GUAR_ST_CD or
T.SFA_CR_PNT_STAT_CD <> S.SFA_CR_PNT_STAT_CD or
T.SFA_CR_CRD_STAT_CD <> S.SFA_CR_CRD_STAT_CD or
T.SFA_CR_RECIP_ID <> S.SFA_CR_RECIP_ID or
T.DATA_ORIGIN = 'D'
when not matched then
insert (
T.EMPLID,
T.INSTITUTION,
T.AID_YEAR,
T.ACAD_CAREER,
T.LOAN_TYPE,
T.LN_APPL_SEQ,
T.ITEM_TYPE,
T.LN_ORIG_ACTN_SEQ,
T.SRC_SYS_ID,
T.LN_ACTION_TYPE,
T.LN_ACTION_CD,
T.LN_ACTION_DT,
T.TRNSFR_BATCH,
T.LN_ACTION_STATUS,
T.LN_ACTNSTAT_DT,
T.DL_LN_APPL_ID_STAT,
T.OPRID,
T.PROCESS_INSTANCE,
T.CL_RECIP_NAME,
T.CL_RECIP_ID_V3,
T.CL_PHASE_CD,
T.SFA_CR_DOCUMENT_ID,
T.SFA_CR_LNDR_ST_CD,
T.SFA_CR_GUAR_ST_CD,
T.SFA_CR_PNT_STAT_CD,
T.SFA_CR_CRD_STAT_CD,
T.SFA_CR_RECIP_ID,
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
S.ACAD_CAREER,
S.LOAN_TYPE,
S.LN_APPL_SEQ,
S.ITEM_TYPE,
S.LN_ORIG_ACTN_SEQ,
'CS90',
S.LN_ACTION_TYPE,
S.LN_ACTION_CD,
S.LN_ACTION_DT,
S.TRNSFR_BATCH,
S.LN_ACTION_STATUS,
S.LN_ACTNSTAT_DT,
S.DL_LN_APPL_ID_STAT,
S.OPRID,
S.PROCESS_INSTANCE,
S.CL_RECIP_NAME,
S.CL_RECIP_ID_V3,
S.CL_PHASE_CD,
S.SFA_CR_DOCUMENT_ID,
S.SFA_CR_LNDR_ST_CD,
S.SFA_CR_GUAR_ST_CD,
S.SFA_CR_PNT_STAT_CD,
S.SFA_CR_CRD_STAT_CD,
S.SFA_CR_RECIP_ID,
'N',
'S',
sysdate,
sysdate,
1234);



strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_LOAN_ORIG_ACTN rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_LOAN_ORIG_ACTN',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_LOAN_ORIG_ACTN';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_LOAN_ORIG_ACTN';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_LOAN_ORIG_ACTN';
update CSSTG_OWNER.PS_LOAN_ORIG_ACTN T
   set T.DATA_ORIGIN = 'D',
       T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, INSTITUTION, AID_YEAR, ACAD_CAREER, LOAN_TYPE, LN_APPL_SEQ, ITEM_TYPE, LN_ORIG_ACTN_SEQ
   from CSSTG_OWNER.PS_LOAN_ORIG_ACTN T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_LOAN_ORIG_ACTN') = 'Y'
  minus
 select EMPLID, INSTITUTION, AID_YEAR, ACAD_CAREER, LOAN_TYPE, LN_APPL_SEQ, ITEM_TYPE, LN_ORIG_ACTN_SEQ
   from SYSADM.PS_LOAN_ORIG_ACTN@SASOURCE S2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_LOAN_ORIG_ACTN') = 'Y'
   ) S
 where T.EMPLID = S.EMPLID
   and T.INSTITUTION = S.INSTITUTION
   and T.AID_YEAR = S.AID_YEAR
   and T.ACAD_CAREER = S.ACAD_CAREER
   and T.LOAN_TYPE = S.LOAN_TYPE
   and T.LN_APPL_SEQ = S.LN_APPL_SEQ
   and T.ITEM_TYPE = S.ITEM_TYPE
   and T.LN_ORIG_ACTN_SEQ = S.LN_ORIG_ACTN_SEQ
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_LOAN_ORIG_ACTN rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_LOAN_ORIG_ACTN',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_LOAN_ORIG_ACTN'
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

END PS_LOAN_ORIG_ACTN_P;
/

DROP PROCEDURE CSMRT_OWNER.PS_TRNSFR_SUBJ_P
/

--
-- PS_TRNSFR_SUBJ_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_TRNSFR_SUBJ_P" AUTHID CURRENT_USER IS


------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_TRNSFR_SUBJ from PeopleSoft table PS_TRNSFR_SUBJ.
--
 --V01  SMT-xxxx 10/10/2017,    James Doucette
--                              Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_TRNSFR_SUBJ';
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
 where TABLE_NAME = 'PS_TRNSFR_SUBJ'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_TRNSFR_SUBJ@SASOURCE S)
 where TABLE_NAME = 'PS_TRNSFR_SUBJ'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_TRNSFR_SUBJ';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_TRNSFR_SUBJ';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_TRNSFR_SUBJ T
using (select /*+ full(S) */
    nvl(trim(INSTITUTION),'-') INSTITUTION, 
    nvl(trim(TRNSFR_SRC_ID),'-') TRNSFR_SRC_ID, 
    nvl(trim(COMP_SUBJECT_AREA),'-') COMP_SUBJECT_AREA, 
    EFFDT, 
    nvl(trim(EFF_STATUS),'-') EFF_STATUS, 
    nvl(trim(DESCR),'-') DESCR, 
    nvl(trim(TC_CATLG_ORG_TYPE),'-') TC_CATLG_ORG_TYPE, 
    nvl(trim(CATALOG_ORG),'-') CATALOG_ORG, 
    nvl(trim(TRNSFR_GRADE_FL),'-') TRNSFR_GRADE_FL, 
    nvl(trim(EXT_TERM_TYPE),'-') EXT_TERM_TYPE, 
    nvl(GRADE_PTS_MIN,0) GRADE_PTS_MIN, 
    nvl(GRADE_PTS_MAX,0) GRADE_PTS_MAX, 
    nvl(UNITS_MINIMUM,0) UNITS_MINIMUM, 
    nvl(UNITS_MAXIMUM,0) UNITS_MAXIMUM, 
    nvl(trim(UNT_TRNSFR_SRC),'-') UNT_TRNSFR_SRC, 
    nvl(trim(CRSE_ID),'-') CRSE_ID, 
    nvl(trim(RQMNT_DESIGNTN),'-') RQMNT_DESIGNTN
from SYSADM.PS_TRNSFR_SUBJ@SASOURCE S 
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_TRNSFR_SUBJ') ) S
 on ( 
    T.INSTITUTION = S.INSTITUTION and 
    T.TRNSFR_SRC_ID = S.TRNSFR_SRC_ID and 
    T.COMP_SUBJECT_AREA = S.COMP_SUBJECT_AREA and 
    T.EFFDT = S.EFFDT and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.EFF_STATUS = S.EFF_STATUS,
    T.DESCR = S.DESCR,
    T.TC_CATLG_ORG_TYPE = S.TC_CATLG_ORG_TYPE,
    T.CATALOG_ORG = S.CATALOG_ORG,
    T.TRNSFR_GRADE_FL = S.TRNSFR_GRADE_FL,
    T.EXT_TERM_TYPE = S.EXT_TERM_TYPE,
    T.GRADE_PTS_MIN = S.GRADE_PTS_MIN,
    T.GRADE_PTS_MAX = S.GRADE_PTS_MAX,
    T.UNITS_MINIMUM = S.UNITS_MINIMUM,
    T.UNITS_MAXIMUM = S.UNITS_MAXIMUM,
    T.UNT_TRNSFR_SRC = S.UNT_TRNSFR_SRC,
    T.CRSE_ID = S.CRSE_ID,
    T.RQMNT_DESIGNTN = S.RQMNT_DESIGNTN,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.EFF_STATUS <> S.EFF_STATUS or 
    T.DESCR <> S.DESCR or 
    T.TC_CATLG_ORG_TYPE <> S.TC_CATLG_ORG_TYPE or 
    T.CATALOG_ORG <> S.CATALOG_ORG or 
    T.TRNSFR_GRADE_FL <> S.TRNSFR_GRADE_FL or 
    T.EXT_TERM_TYPE <> S.EXT_TERM_TYPE or 
    T.GRADE_PTS_MIN <> S.GRADE_PTS_MIN or 
    T.GRADE_PTS_MAX <> S.GRADE_PTS_MAX or 
    T.UNITS_MINIMUM <> S.UNITS_MINIMUM or 
    T.UNITS_MAXIMUM <> S.UNITS_MAXIMUM or 
    T.UNT_TRNSFR_SRC <> S.UNT_TRNSFR_SRC or 
    T.CRSE_ID <> S.CRSE_ID or 
    T.RQMNT_DESIGNTN <> S.RQMNT_DESIGNTN or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.INSTITUTION,
    T.TRNSFR_SRC_ID,
    T.COMP_SUBJECT_AREA,
    T.EFFDT,
    T.SRC_SYS_ID, 
    T.EFF_STATUS, 
    T.DESCR,
    T.TC_CATLG_ORG_TYPE,
    T.CATALOG_ORG,
    T.TRNSFR_GRADE_FL,
    T.EXT_TERM_TYPE,
    T.GRADE_PTS_MIN,
    T.GRADE_PTS_MAX,
    T.UNITS_MINIMUM,
    T.UNITS_MAXIMUM,
    T.UNT_TRNSFR_SRC, 
    T.CRSE_ID,
    T.RQMNT_DESIGNTN, 
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
    ) 
values (
    S.INSTITUTION,
    S.TRNSFR_SRC_ID,
    S.COMP_SUBJECT_AREA,
    S.EFFDT,
    'CS90', 
    S.EFF_STATUS, 
    S.DESCR,
    S.TC_CATLG_ORG_TYPE,
    S.CATALOG_ORG,
    S.TRNSFR_GRADE_FL,
    S.EXT_TERM_TYPE,
    S.GRADE_PTS_MIN,
    S.GRADE_PTS_MAX,
    S.UNITS_MINIMUM,
    S.UNITS_MAXIMUM,
    S.UNT_TRNSFR_SRC, 
    S.CRSE_ID,
    S.RQMNT_DESIGNTN, 
    'N',
    'S',
    sysdate,
    sysdate,
    1234)
;


strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_TRNSFR_SUBJ rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_TRNSFR_SUBJ',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_TRNSFR_SUBJ';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_TRNSFR_SUBJ';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_TRNSFR_SUBJ';
update CSSTG_OWNER.PS_TRNSFR_SUBJ T
   set T.DATA_ORIGIN = 'D',
       T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select INSTITUTION, TRNSFR_SRC_ID, COMP_SUBJECT_AREA, EFFDT
   from CSSTG_OWNER.PS_TRNSFR_SUBJ T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_TRNSFR_SUBJ') = 'Y'
  minus
 select INSTITUTION, TRNSFR_SRC_ID, COMP_SUBJECT_AREA, EFFDT
   from SYSADM.PS_TRNSFR_SUBJ@SASOURCE
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_TRNSFR_SUBJ') = 'Y' 
   ) S
 where T.INSTITUTION = S.INSTITUTION
   and T.TRNSFR_SRC_ID = S.TRNSFR_SRC_ID
   and T.COMP_SUBJECT_AREA = S.COMP_SUBJECT_AREA
   and T.EFFDT = S.EFFDT
   and T.SRC_SYS_ID = 'CS90'    
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_TRNSFR_SUBJ rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_TRNSFR_SUBJ',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_TRNSFR_SUBJ'
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

END PS_TRNSFR_SUBJ_P;
/

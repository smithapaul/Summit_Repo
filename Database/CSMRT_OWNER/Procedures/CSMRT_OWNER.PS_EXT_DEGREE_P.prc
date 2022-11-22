DROP PROCEDURE CSMRT_OWNER.PS_EXT_DEGREE_P
/

--
-- PS_EXT_DEGREE_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_EXT_DEGREE_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
--
-- Loads stage table PS_EXT_DEGREE from PeopleSoft table PS_EXT_DEGREE.
--
-- V01  SMT-xxxx 09/06/2017,    James Doucette
--                              Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_EXT_DEGREE';
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
 where TABLE_NAME = 'PS_EXT_DEGREE'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_EXT_DEGREE@SASOURCE S)
 where TABLE_NAME = 'PS_EXT_DEGREE'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_EXT_DEGREE';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_EXT_DEGREE';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_EXT_DEGREE T 
using (select /*+ full(S) */
    nvl(trim(EMPLID),'-') EMPLID, 
    nvl(trim(EXT_ORG_ID),'-') EXT_ORG_ID, 
    nvl(EXT_DEGREE_NBR,0) EXT_DEGREE_NBR, 
    nvl(trim(DEGREE),'-') DEGREE, 
    nvl(trim(replace(DESCR, '  ', ' ')),'-') DESCR, 
    NVL(DEGREE_DT, to_date(to_char('01/01/1900'), 'MM/DD/YYYY' )) DEGREE_DT, 
    nvl(trim(DEGREE_STATUS),'-') DEGREE_STATUS, 
    nvl(trim(HONORS_CATEGORY),'-') HONORS_CATEGORY, 
    nvl(trim(EXT_SUBJ_AREA_1),'-') EXT_SUBJ_AREA_1, 
    nvl(trim(EXT_SUBJ_AREA_2),'-') EXT_SUBJ_AREA_2, 
    nvl(trim(replace(FIELD_OF_STUDY_1, '  ', ' ')),'-') FIELD_OF_STUDY_1, 
    nvl(trim(FIELD_OF_STUDY_2),'-') FIELD_OF_STUDY_2, 
    nvl(trim(EXT_CAREER),'-') EXT_CAREER, 
    nvl(EXT_DATA_NBR,0) EXT_DATA_NBR, 
    nvl(trim(LS_DATA_SOURCE),'-') LS_DATA_SOURCE
from SYSADM.PS_EXT_DEGREE@SASOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_EXT_DEGREE') ) S 
 on ( 
    T.EMPLID = S.EMPLID and 
    T.EXT_ORG_ID = S.EXT_ORG_ID and 
    T.EXT_DEGREE_NBR = S.EXT_DEGREE_NBR and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.DEGREE = S.DEGREE,
    T.DESCR = S.DESCR,
    T.DEGREE_DT = S.DEGREE_DT,
    T.DEGREE_STATUS = S.DEGREE_STATUS,
    T.HONORS_CATEGORY = S.HONORS_CATEGORY,
    T.EXT_SUBJ_AREA_1 = S.EXT_SUBJ_AREA_1,
    T.EXT_SUBJ_AREA_2 = S.EXT_SUBJ_AREA_2,
    T.FIELD_OF_STUDY_1 = S.FIELD_OF_STUDY_1,
    T.FIELD_OF_STUDY_2 = S.FIELD_OF_STUDY_2,
    T.EXT_CAREER = S.EXT_CAREER,
    T.EXT_DATA_NBR = S.EXT_DATA_NBR,
    T.LS_DATA_SOURCE = S.LS_DATA_SOURCE,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.DEGREE <> S.DEGREE or 
    T.DESCR <> S.DESCR or 
    T.DEGREE_DT <> S.DEGREE_DT or 
    T.DEGREE_STATUS <> S.DEGREE_STATUS or 
    T.HONORS_CATEGORY <> S.HONORS_CATEGORY or 
    T.EXT_SUBJ_AREA_1 <> S.EXT_SUBJ_AREA_1 or 
    T.EXT_SUBJ_AREA_2 <> S.EXT_SUBJ_AREA_2 or 
    T.FIELD_OF_STUDY_1 <> S.FIELD_OF_STUDY_1 or 
    T.FIELD_OF_STUDY_2 <> S.FIELD_OF_STUDY_2 or 
    T.EXT_CAREER <> S.EXT_CAREER or 
    T.EXT_DATA_NBR <> S.EXT_DATA_NBR or 
    T.LS_DATA_SOURCE <> S.LS_DATA_SOURCE or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.EMPLID, 
    T.EXT_ORG_ID, 
    T.EXT_DEGREE_NBR, 
    T.SRC_SYS_ID, 
    T.DEGREE, 
    T.DESCR,
    T.DEGREE_DT,
    T.DEGREE_STATUS,
    T.HONORS_CATEGORY,
    T.EXT_SUBJ_AREA_1,
    T.EXT_SUBJ_AREA_2,
    T.FIELD_OF_STUDY_1, 
    T.FIELD_OF_STUDY_2, 
    T.EXT_CAREER, 
    T.EXT_DATA_NBR, 
    T.LS_DATA_SOURCE, 
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
    ) 
values (
    S.EMPLID, 
    S.EXT_ORG_ID, 
    S.EXT_DEGREE_NBR, 
    'CS90', 
    S.DEGREE, 
    S.DESCR,
    S.DEGREE_DT,
    S.DEGREE_STATUS,
    S.HONORS_CATEGORY,
    S.EXT_SUBJ_AREA_1,
    S.EXT_SUBJ_AREA_2,
    S.FIELD_OF_STUDY_1, 
    S.FIELD_OF_STUDY_2, 
    S.EXT_CAREER, 
    S.EXT_DATA_NBR, 
    S.LS_DATA_SOURCE, 
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

strMessage01    := '# of PS_EXT_DEGREE rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_EXT_DEGREE',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_EXT_DEGREE';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_EXT_DEGREE';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_EXT_DEGREE';
update CSSTG_OWNER.PS_EXT_DEGREE T
   set T.DATA_ORIGIN = 'D',
       T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, EXT_ORG_ID, EXT_DEGREE_NBR
   from CSSTG_OWNER.PS_EXT_DEGREE T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_EXT_DEGREE') = 'Y'
  minus
 select EMPLID, EXT_ORG_ID, EXT_DEGREE_NBR
   from SYSADM.PS_EXT_DEGREE@SASOURCE
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_EXT_DEGREE') = 'Y' 
   ) S
 where T.EMPLID = S.EMPLID   
   AND T.EXT_ORG_ID = S.EXT_ORG_ID
   AND T.EXT_DEGREE_NBR = S.EXT_DEGREE_NBR
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_EXT_DEGREE rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_EXT_DEGREE',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_EXT_DEGREE'
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

END PS_EXT_DEGREE_P;
/

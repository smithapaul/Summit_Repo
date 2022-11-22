DROP PROCEDURE CSMRT_OWNER.AM_PS_EXT_ACAD_SUBJ_P
/

--
-- AM_PS_EXT_ACAD_SUBJ_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."AM_PS_EXT_ACAD_SUBJ_P" IS

------------------------------------------------------------------------
--
-- Loads stage table PS_EXT_ACAD_SUBJ from PeopleSoft table PS_EXT_ACAD_SUBJ.
--
-- V01  SMT-xxxx 09/06/2017,    James Doucette
--                              Converted from DataStage
--VXX    07/06/2021,            Kieu ,Srikanth - Added EMPLID or COMMON_ID additional filter logic 
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'AM_PS_EXT_ACAD_SUBJ';
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
 where TABLE_NAME = 'PS_EXT_ACAD_SUBJ'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_EXT_ACAD_SUBJ@AMSOURCE S)
 where TABLE_NAME = 'PS_EXT_ACAD_SUBJ'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into AMSTG_OWNER.PS_EXT_ACAD_SUBJ';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into AMSTG_OWNER.PS_EXT_ACAD_SUBJ';
merge /*+ use_hash(S,T) */ into AMSTG_OWNER.PS_EXT_ACAD_SUBJ T
using (select /*+ full(S) */
    nvl(trim(EMPLID),'-') EMPLID, 
    nvl(trim(EXT_ORG_ID),'-') EXT_ORG_ID, 
    nvl(trim(EXT_CAREER),'-') EXT_CAREER, 
    nvl(EXT_DATA_NBR,0) EXT_DATA_NBR, 
    nvl(trim(EXT_SUBJECT_AREA),'-') EXT_SUBJECT_AREA, 
    nvl(trim(COURSE_LEVEL),'-') COURSE_LEVEL, 
    nvl(UNT_ATMP_TOTAL,0) UNT_ATMP_TOTAL, 
    nvl(UNT_COMP_TOTAL,0) UNT_COMP_TOTAL, 
    nvl(TOTAL_CRSE_COMP,0) TOTAL_CRSE_COMP, 
    nvl(TOTAL_CRSE_ATMP,0) TOTAL_CRSE_ATMP, 
    nvl(trim(GPA_TYPE),'-') GPA_TYPE, 
    nvl(EXT_GPA,0) EXT_GPA, 
    nvl(CONVERT_GPA,0) CONVERT_GPA, 
    nvl(trim(INSTITUTION),'-') INSTITUTION, 
    nvl(trim(UNT_TYPE),'-') UNT_TYPE
from SYSADM.PS_EXT_ACAD_SUBJ@AMSOURCE S 
where ORA_ROWSCN > (select OLD_MAX_SCN from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_EXT_ACAD_SUBJ') 
  and EMPLID between '00000000' and '99999999'
  and length(EMPLID) = 8 ) S
 on ( 
    T.EMPLID = S.EMPLID and 
    T.EXT_ORG_ID = S.EXT_ORG_ID and 
    T.EXT_CAREER = S.EXT_CAREER and 
    T.EXT_DATA_NBR = S.EXT_DATA_NBR and 
    T.EXT_SUBJECT_AREA = S.EXT_SUBJECT_AREA and 
    T.COURSE_LEVEL = S.COURSE_LEVEL and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.UNT_ATMP_TOTAL = S.UNT_ATMP_TOTAL,
    T.UNT_COMP_TOTAL = S.UNT_COMP_TOTAL,
    T.TOTAL_CRSE_COMP = S.TOTAL_CRSE_COMP,
    T.TOTAL_CRSE_ATMP = S.TOTAL_CRSE_ATMP,
    T.GPA_TYPE = S.GPA_TYPE,
    T.EXT_GPA = S.EXT_GPA,
    T.CONVERT_GPA = S.CONVERT_GPA,
    T.INSTITUTION = S.INSTITUTION,
    T.UNT_TYPE = S.UNT_TYPE,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.UNT_ATMP_TOTAL <> S.UNT_ATMP_TOTAL or 
    T.UNT_COMP_TOTAL <> S.UNT_COMP_TOTAL or 
    T.TOTAL_CRSE_COMP <> S.TOTAL_CRSE_COMP or 
    T.TOTAL_CRSE_ATMP <> S.TOTAL_CRSE_ATMP or 
    T.GPA_TYPE <> S.GPA_TYPE or 
    T.EXT_GPA <> S.EXT_GPA or 
    T.CONVERT_GPA <> S.CONVERT_GPA or 
    T.INSTITUTION <> S.INSTITUTION or 
    T.UNT_TYPE <> S.UNT_TYPE or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.EMPLID, 
    T.EXT_ORG_ID, 
    T.EXT_CAREER, 
    T.EXT_DATA_NBR, 
    T.EXT_SUBJECT_AREA, 
    T.COURSE_LEVEL, 
    T.SRC_SYS_ID, 
    T.UNT_ATMP_TOTAL, 
    T.UNT_COMP_TOTAL, 
    T.TOTAL_CRSE_COMP,
    T.TOTAL_CRSE_ATMP,
    T.GPA_TYPE, 
    T.EXT_GPA,
    T.CONVERT_GPA,
    T.INSTITUTION,
    T.UNT_TYPE, 
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
    ) 
values (
    S.EMPLID, 
    S.EXT_ORG_ID, 
    S.EXT_CAREER, 
    S.EXT_DATA_NBR, 
    S.EXT_SUBJECT_AREA, 
    S.COURSE_LEVEL, 
    'CS90', 
    S.UNT_ATMP_TOTAL, 
    S.UNT_COMP_TOTAL, 
    S.TOTAL_CRSE_COMP,
    S.TOTAL_CRSE_ATMP,
    S.GPA_TYPE, 
    S.EXT_GPA,
    S.CONVERT_GPA,
    S.INSTITUTION,
    S.UNT_TYPE, 
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

strMessage01    := '# of PS_EXT_ACAD_SUBJ rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_EXT_ACAD_SUBJ',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_EXT_ACAD_SUBJ';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on AMSTG_OWNER.PS_EXT_ACAD_SUBJ';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on AMSTG_OWNER.PS_EXT_ACAD_SUBJ';
update AMSTG_OWNER.PS_EXT_ACAD_SUBJ T
        set T.DATA_ORIGIN = 'D',
               T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, EXT_ORG_ID, EXT_CAREER, EXT_DATA_NBR, EXT_SUBJECT_AREA, COURSE_LEVEL
   from AMSTG_OWNER.PS_EXT_ACAD_SUBJ T2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_EXT_ACAD_SUBJ') = 'Y'
  minus
 select EMPLID, EXT_ORG_ID, EXT_CAREER, EXT_DATA_NBR, EXT_SUBJECT_AREA, COURSE_LEVEL
   from SYSADM.PS_EXT_ACAD_SUBJ@AMSOURCE
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_EXT_ACAD_SUBJ') = 'Y' 
   ) S
 where T.EMPLID = S.EMPLID   
   AND T.EXT_ORG_ID = S.EXT_ORG_ID
   AND T.EXT_CAREER = S.EXT_CAREER
   AND T.EXT_DATA_NBR = S.EXT_DATA_NBR
   AND T.EXT_SUBJECT_AREA = S.EXT_SUBJECT_AREA
   AND T.COURSE_LEVEL = S.COURSE_LEVEL
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_EXT_ACAD_SUBJ rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_EXT_ACAD_SUBJ',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on AMSTG_OWNER.UM_STAGE_JOBS';

update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_EXT_ACAD_SUBJ'
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

END AM_PS_EXT_ACAD_SUBJ_P;
/

DROP PROCEDURE CSMRT_OWNER.AM_PS_CRSE_CATALOG_P
/

--
-- AM_PS_CRSE_CATALOG_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER.AM_PS_CRSE_CATALOG_P IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_CRSE_CATALOG from PeopleSoft table PS_CRSE_CATALOG.
--
-- V01  SMT-xxxx 8/18/2017,    Preethi Lodha
--                             Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'AM_PS_CRSE_CATALOG';
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
       START_DT = SYSDATE,
       END_DT = NULL
 where TABLE_NAME = 'PS_CRSE_CATALOG'
;

strSqlCommand := 'commit';
commit;

strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Truncating',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_CRSE_CATALOG@AMSOURCE S)
 where TABLE_NAME = 'PS_CRSE_CATALOG'
;

strSqlCommand := 'commit';
commit;

strSqlDynamic   := 'truncate table AMSTG_OWNER.PS_T_CRSE_CATALOG';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strSqlCommand   := 'Loading temp table for AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Loading'
 where TABLE_NAME = 'PS_CRSE_CATALOG'
;

strSqlCommand := 'commit';
commit;

strSqlCommand := 'insert';
INSERT /*+ append */
      INTO  AMSTG_OWNER.PS_T_CRSE_CATALOG
   SELECT /*+ full(S) */
         CRSE_ID,
          EFFDT,
          'CS90' SRC_SYS_ID,
          EFF_STATUS,
          DESCR,
          EQUIV_CRSE_ID,
          CONSENT,
          ALLOW_MULT_ENROLL,
          UNITS_MINIMUM,
          UNITS_MAXIMUM,
          UNITS_ACAD_PROG,
          UNITS_FINAID_PROG,
          CRSE_REPEATABLE,
          UNITS_REPEAT_LIMIT,
          CRSE_REPEAT_LIMIT,
          GRADING_BASIS,
          GRADE_ROSTER_PRINT,
          SSR_COMPONENT,
          COURSE_TITLE_LONG,
          LST_MULT_TRM_CRS,
          CRSE_CONTACT_HRS,
          RQMNT_DESIGNTN,
          CRSE_COUNT,
          INSTRUCTOR_EDIT,
          FEES_EXIST,
          COMPONENT_PRIMARY,
          ENRL_UN_LD_CLC_TYP,
          '1234' BATCH_SID,
          TO_CHAR (SUBSTR (TRIM (DESCRLONG), 1, 4000)) DESCRLONG,
          TO_NUMBER (ORA_ROWSCN) SRC_SCN
     FROM SYSADM.PS_CRSE_CATALOG@AMSOURCE;

strSqlCommand := 'commit';
commit;

strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging'
 where TABLE_NAME = 'PS_CRSE_CATALOG'
;

strSqlCommand := 'commit';
commit;

strMessage01    := 'Merging data into AMSTG_OWNER.PS_CRSE_CATALOG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into AMSTG_OWNER.PS_CRSE_CATALOG';
merge /*+ use_hash(S,T) */ into AMSTG_OWNER.PS_CRSE_CATALOG T
using (select /*+ full(S) */
nvl(trim(CRSE_ID),'-') CRSE_ID,
to_date(to_char(case when EFFDT < '01-JAN-1800' then to_date('01-JAN-1800') else EFFDT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') EFFDT,   -- Jan 2022 
nvl(trim(EFF_STATUS),'-') EFF_STATUS,
nvl(trim(DESCR),'-') DESCR,
nvl(trim(EQUIV_CRSE_ID),'-') EQUIV_CRSE_ID,
nvl(trim(CONSENT),'-') CONSENT,
nvl(trim(ALLOW_MULT_ENROLL),'-') ALLOW_MULT_ENROLL,
nvl(UNITS_MINIMUM,0) UNITS_MINIMUM,
nvl(UNITS_MAXIMUM,0) UNITS_MAXIMUM,
nvl(UNITS_ACAD_PROG,0) UNITS_ACAD_PROG,
nvl(UNITS_FINAID_PROG,0) UNITS_FINAID_PROG,
nvl(trim(CRSE_REPEATABLE),'-') CRSE_REPEATABLE,
nvl(UNITS_REPEAT_LIMIT,0) UNITS_REPEAT_LIMIT,
nvl(CRSE_REPEAT_LIMIT,0) CRSE_REPEAT_LIMIT,
nvl(trim(GRADING_BASIS),'-') GRADING_BASIS,
nvl(trim(GRADE_ROSTER_PRINT),'-') GRADE_ROSTER_PRINT,
nvl(trim(SSR_COMPONENT),'-') SSR_COMPONENT,
nvl(trim(COURSE_TITLE_LONG),'-') COURSE_TITLE_LONG,
nvl(trim(LST_MULT_TRM_CRS),'-') LST_MULT_TRM_CRS,
nvl(CRSE_CONTACT_HRS,0) CRSE_CONTACT_HRS,
nvl(trim(RQMNT_DESIGNTN),'-') RQMNT_DESIGNTN,
nvl(CRSE_COUNT,0) CRSE_COUNT,
nvl(trim(INSTRUCTOR_EDIT),'-') INSTRUCTOR_EDIT,
nvl(trim(FEES_EXIST),'-') FEES_EXIST,
nvl(trim(COMPONENT_PRIMARY),'-') COMPONENT_PRIMARY,
nvl(trim(ENRL_UN_LD_CLC_TYP),'-') ENRL_UN_LD_CLC_TYP,
DESCRLONG DESCRLONG
from AMSTG_OWNER.PS_T_CRSE_CATALOG S
where SRC_SCN > (select OLD_MAX_SCN from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_CRSE_CATALOG') ) S
   on (
T.CRSE_ID = S.CRSE_ID and
T.EFFDT = S.EFFDT and
T.SRC_SYS_ID = 'CS90')
when matched then update set
T.EFF_STATUS = S.EFF_STATUS,
T.DESCR = S.DESCR,
T.EQUIV_CRSE_ID = S.EQUIV_CRSE_ID,
T.CONSENT = S.CONSENT,
T.ALLOW_MULT_ENROLL = S.ALLOW_MULT_ENROLL,
T.UNITS_MINIMUM = S.UNITS_MINIMUM,
T.UNITS_MAXIMUM = S.UNITS_MAXIMUM,
T.UNITS_ACAD_PROG = S.UNITS_ACAD_PROG,
T.UNITS_FINAID_PROG = S.UNITS_FINAID_PROG,
T.CRSE_REPEATABLE = S.CRSE_REPEATABLE,
T.UNITS_REPEAT_LIMIT = S.UNITS_REPEAT_LIMIT,
T.CRSE_REPEAT_LIMIT = S.CRSE_REPEAT_LIMIT,
T.GRADING_BASIS = S.GRADING_BASIS,
T.GRADE_ROSTER_PRINT = S.GRADE_ROSTER_PRINT,
T.SSR_COMPONENT = S.SSR_COMPONENT,
T.COURSE_TITLE_LONG = S.COURSE_TITLE_LONG,
T.LST_MULT_TRM_CRS = S.LST_MULT_TRM_CRS,
T.CRSE_CONTACT_HRS = S.CRSE_CONTACT_HRS,
T.RQMNT_DESIGNTN = S.RQMNT_DESIGNTN,
T.CRSE_COUNT = S.CRSE_COUNT,
T.INSTRUCTOR_EDIT = S.INSTRUCTOR_EDIT,
T.FEES_EXIST = S.FEES_EXIST,
T.COMPONENT_PRIMARY = S.COMPONENT_PRIMARY,
T.ENRL_UN_LD_CLC_TYP = S.ENRL_UN_LD_CLC_TYP,
T.DESCRLONG = S.DESCRLONG,
T.DATA_ORIGIN = 'S',
T.LASTUPD_EW_DTTM = sysdate,
T.BATCH_SID   = 1234
where
T.EFF_STATUS <> S.EFF_STATUS or
T.DESCR <> S.DESCR or
T.EQUIV_CRSE_ID <> S.EQUIV_CRSE_ID or
T.CONSENT <> S.CONSENT or
T.ALLOW_MULT_ENROLL <> S.ALLOW_MULT_ENROLL or
T.UNITS_MINIMUM <> S.UNITS_MINIMUM or
T.UNITS_MAXIMUM <> S.UNITS_MAXIMUM or
T.UNITS_ACAD_PROG <> S.UNITS_ACAD_PROG or
T.UNITS_FINAID_PROG <> S.UNITS_FINAID_PROG or
T.CRSE_REPEATABLE <> S.CRSE_REPEATABLE or
T.UNITS_REPEAT_LIMIT <> S.UNITS_REPEAT_LIMIT or
T.CRSE_REPEAT_LIMIT <> S.CRSE_REPEAT_LIMIT or
T.GRADING_BASIS <> S.GRADING_BASIS or
T.GRADE_ROSTER_PRINT <> S.GRADE_ROSTER_PRINT or
T.SSR_COMPONENT <> S.SSR_COMPONENT or
T.COURSE_TITLE_LONG <> S.COURSE_TITLE_LONG or
T.LST_MULT_TRM_CRS <> S.LST_MULT_TRM_CRS or
T.CRSE_CONTACT_HRS <> S.CRSE_CONTACT_HRS or
T.RQMNT_DESIGNTN <> S.RQMNT_DESIGNTN or
T.CRSE_COUNT <> S.CRSE_COUNT or
T.INSTRUCTOR_EDIT <> S.INSTRUCTOR_EDIT or
T.FEES_EXIST <> S.FEES_EXIST or
T.COMPONENT_PRIMARY <> S.COMPONENT_PRIMARY or
T.ENRL_UN_LD_CLC_TYP <> S.ENRL_UN_LD_CLC_TYP or
nvl(trim(T.DESCRLONG),0) <> nvl(trim(S.DESCRLONG),0) or
T.DATA_ORIGIN = 'D'
when not matched then
insert (
T.CRSE_ID,
T.EFFDT,
T.SRC_SYS_ID,
T.EFF_STATUS,
T.DESCR,
T.EQUIV_CRSE_ID,
T.CONSENT,
T.ALLOW_MULT_ENROLL,
T.UNITS_MINIMUM,
T.UNITS_MAXIMUM,
T.UNITS_ACAD_PROG,
T.UNITS_FINAID_PROG,
T.CRSE_REPEATABLE,
T.UNITS_REPEAT_LIMIT,
T.CRSE_REPEAT_LIMIT,
T.GRADING_BASIS,
T.GRADE_ROSTER_PRINT,
T.SSR_COMPONENT,
T.COURSE_TITLE_LONG,
T.LST_MULT_TRM_CRS,
T.CRSE_CONTACT_HRS,
T.RQMNT_DESIGNTN,
T.CRSE_COUNT,
T.INSTRUCTOR_EDIT,
T.FEES_EXIST,
T.COMPONENT_PRIMARY,
T.ENRL_UN_LD_CLC_TYP,
T.LOAD_ERROR,
T.DATA_ORIGIN,
T.CREATED_EW_DTTM,
T.LASTUPD_EW_DTTM,
T.BATCH_SID,
T.DESCRLONG
)
values (
S.CRSE_ID,
S.EFFDT,
'CS90',
S.EFF_STATUS,
S.DESCR,
S.EQUIV_CRSE_ID,
S.CONSENT,
S.ALLOW_MULT_ENROLL,
S.UNITS_MINIMUM,
S.UNITS_MAXIMUM,
S.UNITS_ACAD_PROG,
S.UNITS_FINAID_PROG,
S.CRSE_REPEATABLE,
S.UNITS_REPEAT_LIMIT,
S.CRSE_REPEAT_LIMIT,
S.GRADING_BASIS,
S.GRADE_ROSTER_PRINT,
S.SSR_COMPONENT,
S.COURSE_TITLE_LONG,
S.LST_MULT_TRM_CRS,
S.CRSE_CONTACT_HRS,
S.RQMNT_DESIGNTN,
S.CRSE_COUNT,
S.INSTRUCTOR_EDIT,
S.FEES_EXIST,
S.COMPONENT_PRIMARY,
S.ENRL_UN_LD_CLC_TYP,
'N',
'S',
sysdate,
sysdate,
1234,
S.DESCRLONG);

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_CRSE_CATALOG rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_CRSE_CATALOG',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_CRSE_CATALOG';

strSqlCommand := 'commit';
commit;

strMessage01    := 'Updating DATA_ORIGIN on AMSTG_OWNER.PS_CRSE_CATALOG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on AMSTG_OWNER.PS_CRSE_CATALOG';
update AMSTG_OWNER.PS_CRSE_CATALOG T
   set T.DATA_ORIGIN = 'D',
          T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select CRSE_ID, EFFDT
   from AMSTG_OWNER.PS_CRSE_CATALOG T2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_CRSE_CATALOG') = 'Y'
  minus
 select CRSE_ID, EFFDT
   from SYSADM.PS_CRSE_CATALOG@AMSOURCE S2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_CRSE_CATALOG') = 'Y'
   ) S
 where T.CRSE_ID = S.CRSE_ID
   and T.EFFDT = S.EFFDT
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_CRSE_CATALOG rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_CRSE_CATALOG',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on AMSTG_OWNER.UM_STAGE_JOBS';

update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_CRSE_CATALOG'
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

END AM_PS_CRSE_CATALOG_P;
/

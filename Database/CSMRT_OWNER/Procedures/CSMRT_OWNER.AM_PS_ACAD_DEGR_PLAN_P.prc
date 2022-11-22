DROP PROCEDURE CSMRT_OWNER.AM_PS_ACAD_DEGR_PLAN_P
/

--
-- AM_PS_ACAD_DEGR_PLAN_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."AM_PS_ACAD_DEGR_PLAN_P" IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_ACAD_DEGR_PLAN from PeopleSoft table PS_ACAD_DEGR_PLAN.
--
 --V01  SMT-xxxx 06/06/2017,    Preethi Lodha
--                              Converted from PS_ACAD_DEGR_PLAN.SQL
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'AM_PS_ACAD_DEGR_PLAN';
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
 where TABLE_NAME = 'PS_ACAD_DEGR_PLAN'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_ACAD_DEGR_PLAN@AMSOURCE S)
 where TABLE_NAME = 'PS_ACAD_DEGR_PLAN'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into AMSTG_OWNER.PS_ACAD_DEGR_PLAN';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into AMSTG_OWNER.PS_ACAD_DEGR_PLAN';
merge /*+ use_hash(S,T) */ into AMSTG_OWNER.PS_ACAD_DEGR_PLAN T
using (select /*+ full(S) */
nvl(trim(EMPLID),'-') EMPLID,
nvl(trim(STDNT_DEGR),'-') STDNT_DEGR,
nvl(trim(ACAD_PLAN),'-') ACAD_PLAN,
nvl(trim(ACAD_CAREER),'-') ACAD_CAREER,
nvl(STDNT_CAR_NBR,0) STDNT_CAR_NBR,
nvl(trim(ACAD_DEGR_STATUS),'-') ACAD_DEGR_STATUS,
to_date(to_char(case when DEGR_STATUS_DATE < '01-JAN-1800' then NULL else DEGR_STATUS_DATE end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') DEGR_STATUS_DATE,
nvl(trim(OVERRIDE_FL),'-') OVERRIDE_FL,
nvl(trim(DIPLOMA_DESCR),'-') DIPLOMA_DESCR,
nvl(trim(TRNSCR_DESCR),'-') TRNSCR_DESCR,
nvl(trim(HONORS_PREFIX),'-') HONORS_PREFIX,
nvl(trim(HONORS_SUFFIX),'-') HONORS_SUFFIX,
nvl(GPA_PLAN,0) GPA_PLAN,
nvl(CLASS_RANK_NBR,0) CLASS_RANK_NBR,
nvl(CLASS_RANK_TOT,0) CLASS_RANK_TOT,
nvl(PLAN_SEQUENCE,0) PLAN_SEQUENCE,
nvl(trim(OPRID),'-') OPRID
from SYSADM.PS_ACAD_DEGR_PLAN@AMSOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ACAD_DEGR_PLAN') ) S
   on (
T.EMPLID = S.EMPLID and
T.STDNT_DEGR = S.STDNT_DEGR and
T.ACAD_PLAN = S.ACAD_PLAN and
T.SRC_SYS_ID = 'CS90')
when matched then update set
T.ACAD_CAREER = S.ACAD_CAREER,
T.STDNT_CAR_NBR = S.STDNT_CAR_NBR,
T.ACAD_DEGR_STATUS = S.ACAD_DEGR_STATUS,
T.DEGR_STATUS_DATE = S.DEGR_STATUS_DATE,
T.OVERRIDE_FL = S.OVERRIDE_FL,
T.DIPLOMA_DESCR = S.DIPLOMA_DESCR,
T.TRNSCR_DESCR = S.TRNSCR_DESCR,
T.HONORS_PREFIX = S.HONORS_PREFIX,
T.HONORS_SUFFIX = S.HONORS_SUFFIX,
T.GPA_PLAN = S.GPA_PLAN,
T.CLASS_RANK_NBR = S.CLASS_RANK_NBR,
T.CLASS_RANK_TOT = S.CLASS_RANK_TOT,
T.PLAN_SEQUENCE = S.PLAN_SEQUENCE,
T.OPRID = S.OPRID,
T.DATA_ORIGIN = 'S',
T.LASTUPD_EW_DTTM = sysdate,
T.BATCH_SID   = 1234
where
T.ACAD_CAREER <> S.ACAD_CAREER or
T.STDNT_CAR_NBR <> S.STDNT_CAR_NBR or
T.ACAD_DEGR_STATUS <> S.ACAD_DEGR_STATUS or
nvl(trim(T.DEGR_STATUS_DATE),0) <> nvl(trim(S.DEGR_STATUS_DATE),0) or
T.OVERRIDE_FL <> S.OVERRIDE_FL or
T.DIPLOMA_DESCR <> S.DIPLOMA_DESCR or
T.TRNSCR_DESCR <> S.TRNSCR_DESCR or
T.HONORS_PREFIX <> S.HONORS_PREFIX or
T.HONORS_SUFFIX <> S.HONORS_SUFFIX or
T.GPA_PLAN <> S.GPA_PLAN or
T.CLASS_RANK_NBR <> S.CLASS_RANK_NBR or
T.CLASS_RANK_TOT <> S.CLASS_RANK_TOT or
T.PLAN_SEQUENCE <> S.PLAN_SEQUENCE or
T.OPRID <> S.OPRID or
T.DATA_ORIGIN = 'D'
when not matched then
insert (
T.EMPLID,
T.STDNT_DEGR,
T.ACAD_PLAN,
T.SRC_SYS_ID,
T.ACAD_CAREER,
T.STDNT_CAR_NBR,
T.ACAD_DEGR_STATUS,
T.DEGR_STATUS_DATE,
T.OVERRIDE_FL,
T.DIPLOMA_DESCR,
T.TRNSCR_DESCR,
T.HONORS_PREFIX,
T.HONORS_SUFFIX,
T.GPA_PLAN,
T.CLASS_RANK_NBR,
T.CLASS_RANK_TOT,
T.PLAN_SEQUENCE,
T.OPRID,
T.LOAD_ERROR,
T.DATA_ORIGIN,
T.CREATED_EW_DTTM,
T.LASTUPD_EW_DTTM,
T.BATCH_SID
)
values (
S.EMPLID,
S.STDNT_DEGR,
S.ACAD_PLAN,
'CS90',
S.ACAD_CAREER,
S.STDNT_CAR_NBR,
S.ACAD_DEGR_STATUS,
S.DEGR_STATUS_DATE,
S.OVERRIDE_FL,
S.DIPLOMA_DESCR,
S.TRNSCR_DESCR,
S.HONORS_PREFIX,
S.HONORS_SUFFIX,
S.GPA_PLAN,
S.CLASS_RANK_NBR,
S.CLASS_RANK_TOT,
S.PLAN_SEQUENCE,
S.OPRID,
'N',
'S',
sysdate,
sysdate,
1234);
COMMIT;


strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_ACAD_DEGR_PLAN rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_ACAD_DEGR_PLAN',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_ACAD_DEGR_PLAN';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on AMSTG_OWNER.PS_ACAD_DEGR_PLAN';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on AMSTG_OWNER.PS_ACAD_DEGR_PLAN';
update AMSTG_OWNER.PS_ACAD_DEGR_PLAN T
        set T.DATA_ORIGIN = 'D',
               T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, STDNT_DEGR, ACAD_PLAN
   from AMSTG_OWNER.PS_ACAD_DEGR_PLAN T2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ACAD_DEGR_PLAN') = 'Y'
  minus
 select EMPLID, STDNT_DEGR,ACAD_PLAN
   from SYSADM.PS_ACAD_DEGR_PLAN@AMSOURCE
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ACAD_DEGR_PLAN') = 'Y' 

   ) S
 where T.EMPLID = S.EMPLID
   and T.STDNT_DEGR = S.STDNT_DEGR
   AND T.ACAD_PLAN = S.ACAD_PLAN
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_ACAD_DEGR_PLAN rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_ACAD_DEGR_PLAN',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on AMSTG_OWNER.UM_STAGE_JOBS';

update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_ACAD_DEGR_PLAN'
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

END AM_PS_ACAD_DEGR_PLAN_P;
/

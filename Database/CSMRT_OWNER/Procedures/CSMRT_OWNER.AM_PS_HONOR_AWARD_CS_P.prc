DROP PROCEDURE CSMRT_OWNER.AM_PS_HONOR_AWARD_CS_P
/

--
-- AM_PS_HONOR_AWARD_CS_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER.AM_PS_HONOR_AWARD_CS_P IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_HONOR_AWARD_CS from PeopleSoft table PS_HONOR_AWARD_CS.
--
-- V01  SMT-xxxx 8/18/2017,    Preethi Lodha
--                             Converted from DataStage
--
-- V02  SMT-xxxx 12/20/2017,   George Adams
--                             Emergency fix for duplicate source data
--VXX    07/06/2021,            Kieu ,Srikanth - Added EMPLID or COMMON_ID additional filter logic 
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'AM_PS_HONOR_AWARD_CS';
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
 where TABLE_NAME = 'PS_HONOR_AWARD_CS'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Truncating',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_HONOR_AWARD_CS@AMSOURCE S)
 where TABLE_NAME = 'PS_HONOR_AWARD_CS'
;

strSqlCommand := 'commit';
commit;


strSqlDynamic   := 'truncate table AMSTG_OWNER.PS_T_HONOR_AWARD_CS';
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
 where TABLE_NAME = 'PS_HONOR_AWARD_CS'
;

strSqlCommand := 'commit';
commit;


strSqlCommand := 'insert';
INSERT /*+ append */
      INTO  AMSTG_OWNER.PS_T_HONOR_AWARD_CS
   SELECT /*+ full(S) */
         EMPLID,
          'CS90' SRC_SYS_ID,
          DT_RECVD,
          INTERNAL_EXTERNAL,
          INSTITUTION,
          ACAD_CAREER,
          STRM,
          AWARD_CODE,
          DESCRFORMAL,
          GRANTOR,
          ACAD_PROG,
          ACAD_PLAN,
          TRANSCRIPT_LEVEL,
          AWRD_SYS_GENERATED,
          TO_CHAR (SUBSTR (TRIM (COMMENTS), 1, 4000)) COMMENTS,
          TO_NUMBER (ORA_ROWSCN) SRC_SCN
     FROM SYSADM.PS_HONOR_AWARD_CS@AMSOURCE
;
strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging'
 where TABLE_NAME = 'PS_HONOR_AWARD_CS'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into AMSTG_OWNER.PS_HONOR_AWARD_CS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into AMSTG_OWNER.PS_HONOR_AWARD_CS';
merge /*+ use_hash(S,T) */ into AMSTG_OWNER.PS_HONOR_AWARD_CS T
using (with Q1 as (
select /*+ full(S) */
nvl(trim(EMPLID),'-') EMPLID,
to_date(to_char(case when DT_RECVD < '01-JAN-1800' then NULL else DT_RECVD end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') DT_RECVD,
nvl(trim(INTERNAL_EXTERNAL),'-') INTERNAL_EXTERNAL,
nvl(trim(INSTITUTION),'-') INSTITUTION,
nvl(trim(ACAD_CAREER),'-') ACAD_CAREER,
nvl(trim(STRM),'-') STRM,
nvl(trim(AWARD_CODE),'-') AWARD_CODE,
nvl(trim(DESCRFORMAL),'-') DESCRFORMAL,
nvl(trim(GRANTOR),'-') GRANTOR,
nvl(trim(ACAD_PROG),'-') ACAD_PROG,
nvl(trim(ACAD_PLAN),'-') ACAD_PLAN,
nvl(trim(TRANSCRIPT_LEVEL),'-') TRANSCRIPT_LEVEL,
nvl(trim(AWRD_SYS_GENERATED),'-') AWRD_SYS_GENERATED,
COMMENTS COMMENTS,
row_number() over (partition by EMPLID, DT_RECVD, INSTITUTION, ACAD_CAREER, STRM, AWARD_CODE
                       order by AWARD_CODE desc) Q_ORDER
from AMSTG_OWNER.PS_T_HONOR_AWARD_CS S
where SRC_SCN > (select OLD_MAX_SCN from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_HONOR_AWARD_CS')
AND LENGTH(EMPLID) = 8 AND EMPLID BETWEEN '00000000' AND '99999999')
select EMPLID, DT_RECVD, INTERNAL_EXTERNAL, INSTITUTION, ACAD_CAREER, STRM, AWARD_CODE, 
       DESCRFORMAL, GRANTOR, ACAD_PROG, ACAD_PLAN, TRANSCRIPT_LEVEL, AWRD_SYS_GENERATED, COMMENTS
  from Q1
 where Q_ORDER = 1
) S
   on (
T.EMPLID = S.EMPLID and
T.SRC_SYS_ID = 'CS90' and
T.DT_RECVD = S.DT_RECVD and
T.INSTITUTION = S.INSTITUTION and
T.ACAD_CAREER = S.ACAD_CAREER and
T.STRM = S.STRM and
T.AWARD_CODE = S.AWARD_CODE )
when matched then update set
T.INTERNAL_EXTERNAL = S.INTERNAL_EXTERNAL,
T.DESCRFORMAL = S.DESCRFORMAL,
T.GRANTOR = S.GRANTOR,
T.ACAD_PROG = S.ACAD_PROG,
T.ACAD_PLAN = S.ACAD_PLAN,
T.TRANSCRIPT_LEVEL = S.TRANSCRIPT_LEVEL,
T.AWRD_SYS_GENERATED = S.AWRD_SYS_GENERATED,
T.COMMENTS = S.COMMENTS,
T.DATA_ORIGIN = 'S',
T.LASTUPD_EW_DTTM = sysdate,
T.BATCH_SID   = 1234
where
T.INTERNAL_EXTERNAL <> S.INTERNAL_EXTERNAL or
T.DESCRFORMAL <> S.DESCRFORMAL or
T.GRANTOR <> S.GRANTOR or
T.ACAD_PROG <> S.ACAD_PROG or
T.ACAD_PLAN <> S.ACAD_PLAN or
T.TRANSCRIPT_LEVEL <> S.TRANSCRIPT_LEVEL or
T.AWRD_SYS_GENERATED <> S.AWRD_SYS_GENERATED or
nvl(trim(T.COMMENTS),0) <> nvl(trim(S.COMMENTS),0) or
T.DATA_ORIGIN = 'D'
when not matched then
insert (
T.EMPLID,
T.SRC_SYS_ID,
T.DT_RECVD,
T.INTERNAL_EXTERNAL,
T.INSTITUTION,
T.ACAD_CAREER,
T.STRM,
T.AWARD_CODE,
T.DESCRFORMAL,
T.GRANTOR,
T.ACAD_PROG,
T.ACAD_PLAN,
T.TRANSCRIPT_LEVEL,
T.AWRD_SYS_GENERATED,
T.LOAD_ERROR,
T.DATA_ORIGIN,
T.CREATED_EW_DTTM,
T.LASTUPD_EW_DTTM,
T.BATCH_SID,
T.COMMENTS
)
values (
S.EMPLID,
'CS90',
S.DT_RECVD,
S.INTERNAL_EXTERNAL,
S.INSTITUTION,
S.ACAD_CAREER,
S.STRM,
S.AWARD_CODE,
S.DESCRFORMAL,
S.GRANTOR,
S.ACAD_PROG,
S.ACAD_PLAN,
S.TRANSCRIPT_LEVEL,
S.AWRD_SYS_GENERATED,
'N',
'S',
sysdate,
sysdate,
1234,
S.COMMENTS);
strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;


strMessage01    := '# of PS_HONOR_AWARD_CS rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_HONOR_AWARD_CS',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_HONOR_AWARD_CS';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on AMSTG_OWNER.PS_HONOR_AWARD_CS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on AMSTG_OWNER.PS_HONOR_AWARD_CS';
update AMSTG_OWNER.PS_HONOR_AWARD_CS T
   set T.DATA_ORIGIN = 'D',
          T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID,  DT_RECVD, INSTITUTION, ACAD_CAREER, STRM, AWARD_CODE
   from AMSTG_OWNER.PS_HONOR_AWARD_CS T2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_HONOR_AWARD_CS') = 'Y'
  minus
 select EMPLID,  DT_RECVD, INSTITUTION, nvl(trim(ACAD_CAREER),'-'), nvl(trim(STRM),'-'), AWARD_CODE
   from SYSADM.PS_HONOR_AWARD_CS@AMSOURCE S2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_HONOR_AWARD_CS') = 'Y'
   ) S
 where T.EMPLID = S.EMPLID
   and T.DT_RECVD = S.DT_RECVD
    and T.INSTITUTION = S.INSTITUTION
     and  T.ACAD_CAREER = nvl(trim(S.ACAD_CAREER),'-')
      and T.STRM = nvl(trim(S.STRM),'-')
        and T.AWARD_CODE = S.AWARD_CODE
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;
strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_HONOR_AWARD_CS rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_HONOR_AWARD_CS',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on AMSTG_OWNER.UM_STAGE_JOBS';

update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_HONOR_AWARD_CS'
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

END AM_PS_HONOR_AWARD_CS_P;
/

DROP PROCEDURE CSMRT_OWNER.PS_STDNT_ENRL_APPT_P
/

--
-- PS_STDNT_ENRL_APPT_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_STDNT_ENRL_APPT_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_STDNT_ENRL_APPT from PeopleSoft table PS_STDNT_ENRL_APPT.
--54/10/2017,    Jim Doucette 
--                              Converted from PS_STDNT_ENRL_APPT.SQL
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_STDNT_ENRL_APPT';
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
 where TABLE_NAME = 'PS_STDNT_ENRL_APPT'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_STDNT_ENRL_APPT@SASOURCE S)
 where TABLE_NAME = 'PS_STDNT_ENRL_APPT'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_STDNT_ENRL_APPT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_STDNT_ENRL_APPT';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_STDNT_ENRL_APPT T
using (select /*+ full(S) */
    nvl(trim(EMPLID),'-') EMPLID, 
    nvl(trim(ACAD_CAREER),'-') ACAD_CAREER, 
    nvl(trim(INSTITUTION),'-') INSTITUTION, 
    nvl(trim(STRM),'-') STRM, 
    nvl(trim(SESSION_CODE),'-') SESSION_CODE, 
    nvl(trim(SSR_APPT_BLOCK),'-') SSR_APPT_BLOCK, 
    nvl(trim(APPOINTMENT_NBR),'-') APPOINTMENT_NBR, 
    nvl(trim(SSR_SELECT_LIMIT),'-') SSR_SELECT_LIMIT, 
    nvl(trim(APPT_LIMIT_ID),'-') APPT_LIMIT_ID, 
    nvl(MAX_TOTAL_UNIT,0) MAX_TOTAL_UNIT, 
    nvl(MAX_NOGPA_UNIT,0) MAX_NOGPA_UNIT, 
    nvl(MAX_AUDIT_UNIT,0) MAX_AUDIT_UNIT, 
    nvl(MAX_WAIT_UNIT,0) MAX_WAIT_UNIT, 
    nvl(trim(SSR_APPT_STDT_BLCK),'-') SSR_APPT_STDT_BLCK, 
    nvl(trim(INCL_WAIT_IN_TOT),'-') INCL_WAIT_IN_TOT
  from SYSADM.PS_STDNT_ENRL_APPT@SASOURCE S 
 where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_STDNT_ENRL_APPT')
   and EMPLID between '00000000' and '99999999'
   and length(EMPLID) = 8 ) S
 on ( 
    T.EMPLID = S.EMPLID and 
    T.ACAD_CAREER = S.ACAD_CAREER and 
    T.INSTITUTION = S.INSTITUTION and 
    T.STRM = S.STRM and 
    T.SESSION_CODE = S.SESSION_CODE and 
    T.SSR_APPT_BLOCK = S.SSR_APPT_BLOCK and 
    T.APPOINTMENT_NBR = S.APPOINTMENT_NBR and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.SSR_SELECT_LIMIT = S.SSR_SELECT_LIMIT,
    T.APPT_LIMIT_ID = S.APPT_LIMIT_ID,
    T.MAX_TOTAL_UNIT = S.MAX_TOTAL_UNIT,
    T.MAX_NOGPA_UNIT = S.MAX_NOGPA_UNIT,
    T.MAX_AUDIT_UNIT = S.MAX_AUDIT_UNIT,
    T.MAX_WAIT_UNIT = S.MAX_WAIT_UNIT,
    T.SSR_APPT_STDT_BLCK = S.SSR_APPT_STDT_BLCK,
    T.INCL_WAIT_IN_TOT = S.INCL_WAIT_IN_TOT,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.SSR_SELECT_LIMIT <> S.SSR_SELECT_LIMIT or 
    T.APPT_LIMIT_ID <> S.APPT_LIMIT_ID or 
    T.MAX_TOTAL_UNIT <> S.MAX_TOTAL_UNIT or 
    T.MAX_NOGPA_UNIT <> S.MAX_NOGPA_UNIT or 
    T.MAX_AUDIT_UNIT <> S.MAX_AUDIT_UNIT or 
    T.MAX_WAIT_UNIT <> S.MAX_WAIT_UNIT or 
    T.SSR_APPT_STDT_BLCK <> S.SSR_APPT_STDT_BLCK or 
    T.INCL_WAIT_IN_TOT <> S.INCL_WAIT_IN_TOT or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.EMPLID, 
    T.ACAD_CAREER,
    T.INSTITUTION,
    T.STRM, 
    T.SESSION_CODE, 
    T.SSR_APPT_BLOCK, 
    T.APPOINTMENT_NBR,
    T.SRC_SYS_ID, 
    T.SSR_SELECT_LIMIT, 
    T.APPT_LIMIT_ID,
    T.MAX_TOTAL_UNIT, 
    T.MAX_NOGPA_UNIT, 
    T.MAX_AUDIT_UNIT, 
    T.MAX_WAIT_UNIT,
    T.SSR_APPT_STDT_BLCK, 
    T.INCL_WAIT_IN_TOT, 
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
) 
values (
    S.EMPLID, 
    S.ACAD_CAREER,
    S.INSTITUTION,
    S.STRM, 
    S.SESSION_CODE, 
    S.SSR_APPT_BLOCK, 
    S.APPOINTMENT_NBR,
    'CS90', 
    S.SSR_SELECT_LIMIT, 
    S.APPT_LIMIT_ID,
    S.MAX_TOTAL_UNIT, 
    S.MAX_NOGPA_UNIT, 
    S.MAX_AUDIT_UNIT, 
    S.MAX_WAIT_UNIT,
    S.SSR_APPT_STDT_BLCK, 
    S.INCL_WAIT_IN_TOT, 
    'N',
    'S',
    sysdate,
    sysdate,
    1234);


strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_STDNT_ENRL_APPT rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_STDNT_ENRL_APPT',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_STDNT_ENRL_APPT';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_STDNT_ENRL_APPT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_STDNT_ENRL_APPT';
update CSSTG_OWNER.PS_STDNT_ENRL_APPT T
   set T.DATA_ORIGIN = 'D',
          T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, ACAD_CAREER, INSTITUTION, STRM, SESSION_CODE, SSR_APPT_BLOCK, APPOINTMENT_NBR
   from CSSTG_OWNER.PS_STDNT_ENRL_APPT T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_STDNT_ENRL_APPT') = 'Y'
  minus
 select EMPLID, ACAD_CAREER, INSTITUTION, STRM, SESSION_CODE, SSR_APPT_BLOCK, APPOINTMENT_NBR
   from SYSADM.PS_STDNT_ENRL_APPT@SASOURCE S2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_STDNT_ENRL_APPT') = 'Y'
   ) S
 where T.EMPLID = S.EMPLID
   and T.ACAD_CAREER = S.ACAD_CAREER
   and T.INSTITUTION = S.INSTITUTION
   and T.STRM = S.STRM
   and T.SESSION_CODE = S.SESSION_CODE
   and T.SSR_APPT_BLOCK = S.SSR_APPT_BLOCK
   and T.APPOINTMENT_NBR = S.APPOINTMENT_NBR
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_STDNT_ENRL_APPT rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_STDNT_ENRL_APPT',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_STDNT_ENRL_APPT'
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

END PS_STDNT_ENRL_APPT_P;
/

DROP PROCEDURE CSMRT_OWNER.AM_PS_SSR_STDGRD_HONR_P
/

--
-- AM_PS_SSR_STDGRD_HONR_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."AM_PS_SSR_STDGRD_HONR_P" IS

------------------------------------------------------------------------
-- James Doucette
--
-- Loads stage table PS_SSR_STDGRD_HONR from PeopleSoft table PS_SSR_STDGRD_HONR.
--
-- V01  Case: 79169  11/12/2020,    James Doucette
--                                  
--VXX    07/06/2021,            Kieu ,Srikanth - Added EMPLID or COMMON_ID additional filter logic 
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'AM_PS_SSR_STDGRD_HONR';
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
 where TABLE_NAME = 'PS_SSR_STDGRD_HONR'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_SSR_STDGRD_HONR@AMSOURCE S)
 where TABLE_NAME = 'PS_SSR_STDGRD_HONR'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into AMSTG_OWNER.PS_SSR_STDGRD_HONR';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into AMSTG_OWNER.PS_SSR_STDGRD_HONR';
merge /*+ use_hash(S,T) */ into AMSTG_OWNER.PS_SSR_STDGRD_HONR T 
using (select /*+ full(S) */
    nvl(trim(EMPLID),'-') EMPLID, 
	nvl(trim(INSTITUTION),'-') INSTITUTION,
	nvl(trim(ACAD_CAREER),'-') ACAD_CAREER, 
	STDNT_CAR_NBR, 
	nvl(trim(ACAD_PROG),'-') ACAD_PROG, 
	nvl(trim(EXP_GRAD_TERM),'-') EXP_GRAD_TERM, 
	nvl(trim(DEGREE),'-') DEGREE, 
	SEQNUM,
	HONORS_CODE
from SYSADM.PS_SSR_STDGRD_HONR@AMSOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_SSR_STDGRD_HONR') 
 and EMPLID between '00000000' and '99999999'
   and length(EMPLID) = 8
) S 
 on ( 
    T.EMPLID = S.EMPLID and 
    T.INSTITUTION = S.INSTITUTION and 
	T.ACAD_CAREER = S.ACAD_CAREER and 
	T.STDNT_CAR_NBR = S.STDNT_CAR_NBR and 
	T.ACAD_PROG = S.ACAD_PROG and 
	T.EXP_GRAD_TERM = S.EXP_GRAD_TERM and 
	T.DEGREE = S.DEGREE and 
	T.SEQNUM = S.SEQNUM and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.HONORS_CODE = S.HONORS_CODE,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate
where 
    T.HONORS_CODE <> S.HONORS_CODE or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.EMPLID,
    T.INSTITUTION,
    T.ACAD_CAREER, 
    T.STDNT_CAR_NBR, 
    T.ACAD_PROG,
    T.EXP_GRAD_TERM, 
    T.DEGREE, 
	T.SEQNUM,
	T.SRC_SYS_ID,
	T.HONORS_CODE,
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM
    ) 
values (
    S.EMPLID,
    S.INSTITUTION,
    S.ACAD_CAREER, 
    S.STDNT_CAR_NBR, 
    S.ACAD_PROG,
    S.EXP_GRAD_TERM, 
    S.DEGREE, 
	S.SEQNUM,	
    'CS90', 
    S.HONORS_CODE, 
    'S',
    sysdate,
    sysdate
	)
;


strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_SSR_STDGRD_HONR rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_SSR_STDGRD_HONR',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_SSR_STDGRD_HONR';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on AMSTG_OWNER.PS_SSR_STDGRD_HONR';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on AMSTG_OWNER.PS_SSR_STDGRD_HONR';
update AMSTG_OWNER.PS_SSR_STDGRD_HONR T
        set T.DATA_ORIGIN = 'D',
            T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, INSTITUTION, ACAD_CAREER, STDNT_CAR_NBR, ACAD_PROG, EXP_GRAD_TERM, DEGREE, SEQNUM
   from AMSTG_OWNER.PS_SSR_STDGRD_HONR T2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_SSR_STDGRD_HONR') = 'Y'
  minus
 select EMPLID, INSTITUTION, ACAD_CAREER, STDNT_CAR_NBR, ACAD_PROG, EXP_GRAD_TERM, DEGREE, SEQNUM
   from SYSADM.PS_SSR_STDGRD_HONR@AMSOURCE
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_SSR_STDGRD_HONR') = 'Y' 
   ) S
 where T.EMPLID = S.EMPLID
   and T.INSTITUTION = S.INSTITUTION
   and T.ACAD_CAREER = S.ACAD_CAREER
   and T.STDNT_CAR_NBR = S.STDNT_CAR_NBR
   and T.ACAD_PROG = S.ACAD_PROG
   and T.EXP_GRAD_TERM = S.EXP_GRAD_TERM
   and T.DEGREE = S.DEGREE
   and T.SEQNUM = S.SEQNUM
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_SSR_STDGRD_HONR rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_SSR_STDGRD_HONR',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on AMSTG_OWNER.UM_STAGE_JOBS';

update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_SSR_STDGRD_HONR'
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

END AM_PS_SSR_STDGRD_HONR_P;
/

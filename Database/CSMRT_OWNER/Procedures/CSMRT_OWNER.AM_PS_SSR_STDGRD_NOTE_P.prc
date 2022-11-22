DROP PROCEDURE CSMRT_OWNER.AM_PS_SSR_STDGRD_NOTE_P
/

--
-- AM_PS_SSR_STDGRD_NOTE_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."AM_PS_SSR_STDGRD_NOTE_P" IS

   ------------------------------------------------------------------------
   -- George Adams
   --
   -- Loads stage table PS_SSR_STDGRD_NOTE from PeopleSoft table PS_SSR_STDGRD_NOTE.
   --
   -- V01  SMT-7992 09/12/2018,    James Doucette
   --                              New stage table from PeopleSoft.
   --VXX    07/06/2021,            Kieu ,Srikanth - Added EMPLID or COMMON_ID additional filter logic 
   ------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'AM_PS_SSR_STDGRD_NOTE';
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
 where TABLE_NAME = 'PS_SSR_STDGRD_NOTE'
;

strSqlCommand := 'commit';
commit;

strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Truncating',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_SSR_STDGRD_NOTE@AMSOURCE S)
 where TABLE_NAME = 'PS_SSR_STDGRD_NOTE'
;

strSqlCommand := 'commit';
commit;

strSqlDynamic   := 'truncate table AMSTG_OWNER.PS_T_SSR_STDGRD_NOTE';
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
 where TABLE_NAME = 'PS_SSR_STDGRD_NOTE'
;

strSqlCommand := 'commit';
commit;

strSqlCommand := 'insert';
INSERT /*+ append */
      INTO  AMSTG_OWNER.PS_T_SSR_STDGRD_NOTE
SELECT /*+ full(S) */
        EMPLID, 
		INSTITUTION, 
		ACAD_CAREER, 
		STDNT_CAR_NBR, 
		ACAD_PROG, 
		EXP_GRAD_TERM, 
		DEGREE, 
		SEQNUM, 
		'CS90' SRC_SYS_ID, 
		SSR_GRAD_NOTE, 
		SSR_DESCRIPTION, 
		'-' SCC_ROW_ADD_OPRID,     -- Jan 2022  
		NULL SCC_ROW_ADD_DTTM, 
		'-' SCC_ROW_UPD_OPRID, 
		NULL SCC_ROW_UPD_DTTM, 
        to_char(substr(trim(SSR_GRAD_NOTE_LONG), 1, 4000))  SSR_GRAD_NOTE_LONG,
        to_number(ORA_ROWSCN) SRC_SCN
   FROM SYSADM.PS_SSR_STDGRD_NOTE@AMSOURCE S
;

strSqlCommand := 'commit';
commit;

strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging'
 where TABLE_NAME = 'PS_SSR_STDGRD_NOTE'
;

strSqlCommand := 'commit';
commit;

strMessage01    := 'Merging data into AMSTG_OWNER.PS_SSR_STDGRD_NOTE';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into AMSTG_OWNER.PS_SSR_STDGRD_NOTE';
merge /*+ use_hash(S,T) */ into AMSTG_OWNER.PS_SSR_STDGRD_NOTE T 
using (select /*+ full(S) */ 
	nvl(trim(EMPLID),'-') EMPLID, 
	nvl(trim(INSTITUTION),'-') INSTITUTION, 
	nvl(trim(ACAD_CAREER),'-') ACAD_CAREER, 
	nvl(STDNT_CAR_NBR,0) STDNT_CAR_NBR, 
	nvl(trim(ACAD_PROG),'-') ACAD_PROG, 
	nvl(trim(EXP_GRAD_TERM),'-') EXP_GRAD_TERM, 
	nvl(trim(DEGREE),'-') DEGREE, 
	nvl(SEQNUM,0) SEQNUM, 
	nvl(trim(SSR_GRAD_NOTE),'-') SSR_GRAD_NOTE, 
	nvl(trim(SSR_DESCRIPTION),'-') SSR_DESCRIPTION, 
	nvl(trim(SCC_ROW_ADD_OPRID),'-') SCC_ROW_ADD_OPRID, 
	SCC_ROW_ADD_DTTM SCC_ROW_ADD_DTTM, 
	nvl(trim(SCC_ROW_UPD_OPRID),'-') SCC_ROW_UPD_OPRID, 
	SCC_ROW_UPD_DTTM SCC_ROW_UPD_DTTM, 
	SSR_GRAD_NOTE_LONG SSR_GRAD_NOTE_LONG
from AMSTG_OWNER.PS_T_SSR_STDGRD_NOTE S 
where ORA_ROWSCN > (select OLD_MAX_SCN from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_SSR_STDGRD_NOTE') 
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
	T.SSR_GRAD_NOTE = S.SSR_GRAD_NOTE, 
	T.SSR_DESCRIPTION = S.SSR_DESCRIPTION, 
	T.SCC_ROW_ADD_OPRID = S.SCC_ROW_ADD_OPRID, 
	T.SCC_ROW_ADD_DTTM = S.SCC_ROW_ADD_DTTM, 
	T.SCC_ROW_UPD_OPRID = S.SCC_ROW_UPD_OPRID, 
	T.SCC_ROW_UPD_DTTM = S.SCC_ROW_UPD_DTTM, 
	T.SSR_GRAD_NOTE_LONG = S.SSR_GRAD_NOTE_LONG, 
	T.DATA_ORIGIN = 'S', 
	T.LASTUPD_EW_DTTM = sysdate, 
	T.BATCH_SID = 1234 
where 
	T.SSR_GRAD_NOTE <> S.SSR_GRAD_NOTE or 
	T.SSR_DESCRIPTION <> S.SSR_DESCRIPTION or 
	T.SCC_ROW_ADD_OPRID <> S.SCC_ROW_ADD_OPRID or 
	nvl(trim(T.SCC_ROW_ADD_DTTM),0) <> nvl(trim(S.SCC_ROW_ADD_DTTM),0) or 
	T.SCC_ROW_UPD_OPRID <> S.SCC_ROW_UPD_OPRID or 
	nvl(trim(T.SCC_ROW_UPD_DTTM),0) <> nvl(trim(S.SCC_ROW_UPD_DTTM),0) or 
	nvl(trim(T.SSR_GRAD_NOTE_LONG),0) <> nvl(trim(S.SSR_GRAD_NOTE_LONG),0) or 
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
	T.SSR_GRAD_NOTE, 
	T.SSR_DESCRIPTION, 
	T.SCC_ROW_ADD_OPRID, 
	T.SCC_ROW_ADD_DTTM, 
	T.SCC_ROW_UPD_OPRID, 
	T.SCC_ROW_UPD_DTTM, 
	T.LOAD_ERROR, 
	T.DATA_ORIGIN, 
	T.CREATED_EW_DTTM, 
	T.LASTUPD_EW_DTTM, 
	T.BATCH_SID, 
	T.SSR_GRAD_NOTE_LONG
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
	S.SSR_GRAD_NOTE, 
	S.SSR_DESCRIPTION, 
	S.SCC_ROW_ADD_OPRID, 
	S.SCC_ROW_ADD_DTTM, 
	S.SCC_ROW_UPD_OPRID, 
	S.SCC_ROW_UPD_DTTM, 
	'N', 
	'S', 
	sysdate, 
	sysdate, 
	1234,
	S.SSR_GRAD_NOTE_LONG)
	; 

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_SSR_STDGRD_NOTE rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_SSR_STDGRD_NOTE',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_SSR_STDGRD_NOTE';

strSqlCommand := 'commit';
commit;

strMessage01    := 'Updating DATA_ORIGIN on AMSTG_OWNER.PS_SSR_STDGRD_NOTE';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on AMSTG_OWNER.PS_SSR_STDGRD_NOTE';
update AMSTG_OWNER.PS_SSR_STDGRD_NOTE T
   set T.DATA_ORIGIN = 'D',
          T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select nvl(trim(EMPLID),'-') EMPLID, 
	    nvl(trim(INSTITUTION),'-') INSTITUTION, 
	    nvl(trim(ACAD_CAREER),'-') ACAD_CAREER, 
	    nvl(STDNT_CAR_NBR,0) STDNT_CAR_NBR, 
	    nvl(trim(ACAD_PROG),'-') ACAD_PROG, 
	    nvl(trim(EXP_GRAD_TERM),'-') EXP_GRAD_TERM, 
	    nvl(trim(DEGREE),'-') DEGREE, 
	    nvl(SEQNUM,0) SEQNUM
   from AMSTG_OWNER.PS_SSR_STDGRD_NOTE T2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_SSR_STDGRD_NOTE') = 'Y'
  minus
 select EMPLID, INSTITUTION, ACAD_CAREER, STDNT_CAR_NBR, ACAD_PROG, nvl(trim(EXP_GRAD_TERM),'-') EXP_GRAD_TERM, DEGREE, SEQNUM
   from SYSADM.PS_SSR_STDGRD_NOTE@AMSOURCE S2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_SSR_STDGRD_NOTE') = 'Y'
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

strMessage01    := '# of PS_SSR_STDGRD_NOTE rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_SSR_STDGRD_NOTE',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on AMSTG_OWNER.UM_STAGE_JOBS';

update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_SSR_STDGRD_NOTE'
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

END AM_PS_SSR_STDGRD_NOTE_P;
/

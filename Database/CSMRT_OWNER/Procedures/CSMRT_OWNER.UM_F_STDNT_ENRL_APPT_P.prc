CREATE OR REPLACE PROCEDURE             "UM_F_STDNT_ENRL_APPT_P" AUTHID CURRENT_USER IS


------------------------------------------------------------------------
-- George Adams
--
-- Loads mart table UM_F_STDNT_ENRL_APPT.
--
 --V01  SMT-xxxx 07/03/2018,    James Doucette
--                              Converted from SQL Script
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_STDNT_ENRL_APPT';
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

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_STDNT_ENRL_APPT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_STDNT_ENRL_APPT');

--alter table UM_F_STDNT_ENRL_APPT disable constraint PK_UM_F_STDNT_ENRL_APPT;
strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_STDNT_ENRL_APPT disable constraint PK_UM_F_STDNT_ENRL_APPT';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
				
strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_STDNT_ENRL_APPT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_STDNT_ENRL_APPT';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );


strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_STDNT_ENRL_APPT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_STDNT_ENRL_APPT';				
insert into CSMRT_OWNER.UM_F_STDNT_ENRL_APPT
with APPT as (
select INSTITUTION, ACAD_CAREER, STRM, SESSION_CODE, SSR_APPT_BLOCK, APPOINTMENT_NBR, SRC_SYS_ID, 
       to_date(to_char(APPT_START_DATE,'YYYYMMDD')||to_char(APPT_START_TIME,'HH24MISS'),'YYYYMMDDHH24MISS') APPT_START_DTTM, 
       to_date(to_char(APPT_END_DATE,'YYYYMMDD')||to_char(APPT_END_TIME,'HH24MISS'),'YYYYMMDDHH24MISS') APPT_END_DTTM 
  from CSSTG_OWNER.PS_APPOINTMENT_TBL
 where DATA_ORIGIN <> 'D'
)
select E.INSTITUTION INSTITUTION_CD, E.ACAD_CAREER ACAD_CAR_CD, E.STRM TERM_CD, E.EMPLID PERSON_ID, E.SESSION_CODE, E.SSR_APPT_BLOCK, E.APPOINTMENT_NBR, E.SRC_SYS_ID, 
       nvl(I.INSTITUTION_SID, 2147483646) INSTITUTION_SID, 
       nvl(C.ACAD_CAR_SID, 2147483646) ACAD_CAR_SID, 
       nvl(T.TERM_SID, 2147483646) TERM_SID, 
       nvl(P.PERSON_SID, 2147483646) PERSON_SID, 
       nvl(S.SESSION_SID, 2147483646) SESSION_SID, 
       A.APPT_START_DTTM,
       A.APPT_END_DTTM, 
       E.SSR_SELECT_LIMIT, E.APPT_LIMIT_ID, E.MAX_TOTAL_UNIT, E.MAX_NOGPA_UNIT, E.MAX_AUDIT_UNIT, E.MAX_WAIT_UNIT, E.SSR_APPT_STDT_BLCK, E.INCL_WAIT_IN_TOT, 
       'N' LOAD_ERROR, 
       'S' DATA_ORIGIN, 
       SYSDATE CREATED_EW_DTTM, 
       SYSDATE LASTUPD_EW_DTTM, 
       1234 BATCH_SID
  from CSSTG_OWNER.PS_STDNT_ENRL_APPT E
  left outer join APPT A
    on E.INSTITUTION = A.INSTITUTION
   and E.ACAD_CAREER = A.ACAD_CAREER
   and E.STRM = A.STRM
   and E.SESSION_CODE = A.SESSION_CODE
   and E.SSR_APPT_BLOCK = A.SSR_APPT_BLOCK
   and E.APPOINTMENT_NBR = A.APPOINTMENT_NBR
   and E.SRC_SYS_ID = A.SRC_SYS_ID
  left outer join CSMRT_OWNER.PS_D_INSTITUTION I
    on E.INSTITUTION = I.INSTITUTION_CD
   and E.SRC_SYS_ID = I.SRC_SYS_ID
   and I.DATA_ORIGIN <> 'D'
  left outer join CSMRT_OWNER.PS_D_ACAD_CAR C
    on E.INSTITUTION = C.INSTITUTION_CD
   and E.ACAD_CAREER = C.ACAD_CAR_CD
   and E.SRC_SYS_ID = C.SRC_SYS_ID
   and C.DATA_ORIGIN <> 'D'
  left outer join CSMRT_OWNER.PS_D_TERM T
    on E.INSTITUTION = T.INSTITUTION_CD
   and E.ACAD_CAREER = T.ACAD_CAR_CD
   and E.STRM = T.TERM_CD
   and E.SRC_SYS_ID = C.SRC_SYS_ID
   and T.DATA_ORIGIN <> 'D'
  left outer join CSMRT_OWNER.PS_D_PERSON P
    on E.EMPLID = P.PERSON_ID
   and E.SRC_SYS_ID = P.SRC_SYS_ID
   and P.DATA_ORIGIN <> 'D'
  left outer join CSMRT_OWNER.PS_D_SESSION S
    on E.INSTITUTION = S.INSTITUTION_CD
   and E.ACAD_CAREER = S.ACAD_CAR_CD
   and E.STRM = S.TERM_CD
   and E.SESSION_CODE = S.SESSION_CD
   and E.SRC_SYS_ID = S.SRC_SYS_ID
   and S.DATA_ORIGIN <> 'D'
 where E.DATA_ORIGIN <> 'D'
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_STDNT_ENRL_APPT rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_STDNT_ENRL_APPT',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );



strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_STDNT_ENRL_APPT',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_STDNT_ENRL_APPT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
--alter table UM_F_STDNT_ENRL_APPT enable constraint PK_UM_F_STDNT_ENRL_APPT;

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_STDNT_ENRL_APPT enable constraint PK_UM_F_STDNT_ENRL_APPT';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_STDNT_ENRL_APPT');

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

END UM_F_STDNT_ENRL_APPT_P;
/

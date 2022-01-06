CREATE OR REPLACE PROCEDURE             "UM_F_ADM_APPL_ENRL_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
--George Adams
--Loads table                -- UM_F_ADM_APPL_ENRL
--V01 12/12/2018             -- srikanth ,pabbu converted to proc from sql scripts

------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_ADM_APPL_ENRL';
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

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_ADM_APPL_ENRL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_ADM_APPL_ENRL');

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_ADM_APPL_ENRL disable constraint PK_UM_F_ADM_APPL_ENRL';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_ADM_APPL_ENRL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_ADM_APPL_ENRL';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_ADM_APPL_ENRL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_ADM_APPL_ENRL';				
insert /*+ append */ into UM_F_ADM_APPL_ENRL
with TERM as (
select A.TERM_SID, 
       A.INSTITUTION_CD, A.ACAD_CAR_CD, A.TERM_CD, A.SRC_SYS_ID, 
       A.PREV_TERM, 
       B.TERM_SID PREV_TERM_SID, 
       A.PREV_TERM_2, 
       C.TERM_SID PREV_TERM_2_SID, 
       A.NEXT_TERM,
       D.TERM_SID NEXT_TERM_SID,
       A.NEXT_TERM_2,                         -- Added!!! 
       E.TERM_SID NEXT_TERM_2_SID
  from UM_D_TERM_VW A 
  left outer join PS_D_TERM B
    on A.INSTITUTION_CD = B.INSTITUTION_CD
   and A.ACAD_CAR_CD = B.ACAD_CAR_CD
   and A.PREV_TERM = B.TERM_CD
   and A.SRC_SYS_ID = B.SRC_SYS_ID
  left outer join PS_D_TERM C
    on A.INSTITUTION_CD = C.INSTITUTION_CD
   and A.ACAD_CAR_CD = C.ACAD_CAR_CD
   and A.PREV_TERM_2 = C.TERM_CD
   and A.SRC_SYS_ID = C.SRC_SYS_ID
  left outer join PS_D_TERM D 
    on A.INSTITUTION_CD = D.INSTITUTION_CD
   and A.ACAD_CAR_CD = D.ACAD_CAR_CD
   and A.NEXT_TERM = D.TERM_CD
   and A.SRC_SYS_ID = D.SRC_SYS_ID
  left outer join PS_D_TERM E 
    on A.INSTITUTION_CD = E.INSTITUTION_CD
   and A.ACAD_CAR_CD = E.ACAD_CAR_CD
   and A.NEXT_TERM_2 = E.TERM_CD
   and A.SRC_SYS_ID = E.SRC_SYS_ID
),
ENRL as (
select F.TERM_SID, F.PERSON_SID, F2.STDNT_CAR_NUM, F.SRC_SYS_ID, F.TERM_CD,   
       (CASE WHEN SUM(CASE WHEN S.ENRLMT_STAT_ID = 'E' THEN F.TAKEN_UNIT ELSE 0 END) > 0
             THEN 1 ELSE 0 END) ENROLL_CNT
  from UM_F_CLASS_ENRLMT F, PS_D_ENRLMT_STAT S, UM_F_ACAD_PROG F2 
 where F.ENRLMT_STAT_SID = S.ENRLMT_STAT_SID
   and F.TERM_SID = F2.TERM_SID
   and F.PERSON_SID = F2.PERSON_SID 
   and F.SRC_SYS_ID = F2.SRC_SYS_ID
   and F.TERM_CD <> '-'             -- May 2017 
 group by F.TERM_SID, F.PERSON_SID, F2.STDNT_CAR_NUM, F.SRC_SYS_ID, F.TERM_CD
)
select ADM.ADM_APPL_SID,
       TERM.INSTITUTION_CD,
       TERM.ACAD_CAR_CD, 
       TERM.TERM_CD ADMIT_TERM_CD,
       TERM.SRC_SYS_ID, 
       coalesce(ENRL1.TERM_CD,ENRL2.TERM_CD) PREV_TERM_CD,
       coalesce(ENRL1.TERM_SID,ENRL2.TERM_SID) PREV_TERM_SID,
--       ENRL3.TERM_CD NEXT_TERM_CD,
--       ENRL3.TERM_SID NEXT_TERM_SID,
       coalesce(ENRL3.TERM_CD,ENRL4.TERM_CD) NEXT_TERM_CD,
       coalesce(ENRL3.TERM_SID,ENRL4.TERM_SID) NEXT_TERM_SID,
       nvl(ADM.ENROLL_CNT,0) ENROLL_CNT,
       nvl(coalesce(ENRL1.ENROLL_CNT, ENRL2.ENROLL_CNT),0) PREV_ENROLL_CNT, 
--       nvl(ENRL3.ENROLL_CNT,0) NEXT_ENROLL_CNT
       nvl(coalesce(ENRL3.ENROLL_CNT, ENRL4.ENROLL_CNT),0) NEXT_ENROLL_CNT 
  from UM_F_ADM_APPL_STAT ADM
  join TERM
    on ADM.ADMIT_TERM_SID = TERM.TERM_SID
  left outer join ENRL ENRL1
    on ADM.APPLCNT_SID = ENRL1.PERSON_SID
   and ADM.STU_CAR_NBR_SR = ENRL1.STDNT_CAR_NUM  
   and TERM.PREV_TERM_SID = ENRL1.TERM_SID  
   and ENRL1.ENROLL_CNT > 0  
  left outer join ENRL ENRL2
    on ADM.APPLCNT_SID = ENRL2.PERSON_SID
   and ADM.STU_CAR_NBR_SR = ENRL2.STDNT_CAR_NUM  
   and TERM.PREV_TERM_2_SID = ENRL2.TERM_SID  
   and ENRL2.ENROLL_CNT > 0  
  left outer join ENRL ENRL3
    on ADM.APPLCNT_SID = ENRL3.PERSON_SID
   and ADM.STU_CAR_NBR_SR = ENRL3.STDNT_CAR_NUM  
   and TERM.NEXT_TERM_SID = ENRL3.TERM_SID  
   and ENRL3.ENROLL_CNT > 0  
  left outer join ENRL ENRL4
    on ADM.APPLCNT_SID = ENRL4.PERSON_SID
   and ADM.STU_CAR_NBR_SR = ENRL4.STDNT_CAR_NUM  
   and TERM.NEXT_TERM_2_SID = ENRL4.TERM_SID  
   and ENRL4.ENROLL_CNT > 0  
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_ADM_APPL_ENRL rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_ADM_APPL_ENRL',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_ADM_APPL_ENRL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_ADM_APPL_ENRL enable constraint PK_UM_F_ADM_APPL_ENRL';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_ADM_APPL_ENRL');

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

END UM_F_ADM_APPL_ENRL_P;
/

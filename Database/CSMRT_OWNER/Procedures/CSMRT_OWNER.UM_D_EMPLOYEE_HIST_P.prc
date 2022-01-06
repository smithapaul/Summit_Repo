CREATE OR REPLACE PROCEDURE             "UM_D_EMPLOYEE_HIST_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
--George Adams
--Loads table                -- UM_D_EMPLOYEE_HIST
--V01 12/12/2018             -- srikanth ,pabbu converted to proc from sql scripts

------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_D_EMPLOYEE_HIST';
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

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_D_EMPLOYEE_HIST';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_D_EMPLOYEE_HIST');

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_D_EMPLOYEE_HIST disable constraint PK_UM_D_EMPLOYEE_HIST';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
strMessage01    := 'Truncating table CSMRT_OWNER.UM_D_EMPLOYEE_HIST';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_D_EMPLOYEE_HIST';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_D_EMPLOYEE_HIST';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_D_EMPLOYEE_HIST';				
insert into UM_D_EMPLOYEE_HIST
with
EXT as (
select /*+ INLINE PARALLEL(8) */
       EMPLID, SRC_SYS_ID, EXTERNAL_SYSTEM_ID HR_EMPLID,  
       ROW_NUMBER() OVER (PARTITION BY EMPLID, SRC_SYS_ID  
                              ORDER BY EFFDT desc) EXT_ORDER
  from CSSTG_OWNER.PS_EXTERNAL_SYSTEM
 where DATA_ORIGIN <> 'D'
   and SRC_SYS_ID = 'CS90'
   and EXTERNAL_SYSTEM = 'HR'
   and length(trim(EMPLID)) = 8
   and trim(EMPLID) between '00000000' and '99999999'),
JOB as (
select /*+ INLINE PARALLEL(8) */
       EMPLID, EMPL_RCD, EFF_START_DT, EFFSEQ, 
       EFF_END_DT, CURRENT_IND, BU_SID, JOBCODE_SID, 
       EMPL_STAT_CD, EMPL_STAT_LD,
       EMPL_TYPE_CD, EMPL_TYPE_LD, 
       FULL_PT_CD, FULL_PT_LD, 
       HR_STAT_CD, HR_STAT_LD, 
       J.REG_TEMP_CD, J.REG_TEMP_LD,
       row_number() over (partition by PERSON_SID, EMPL_RCD, EFFDT
                              order by EFFSEQ desc) JOB_EFFSEQ_ORDER    -- Need??? 
  from HRMRT_OWNER.PS_D_EMPL_JOB J
 where DELETED_FLAG = 'N'
   and MAX_EFFSEQ_FLAG = 'Y'
   and PER_ORG_TYPE_CD = 'EMP') 
select P.PERSON_SID,
       EXT.EMPLID PERSON_ID,
       J.EMPL_RCD, 
       J.EFF_START_DT, 
       J.EFFSEQ,
       P.SRC_SYS_ID, 
       J.EMPLID HR_EMPLID, 
       J.EFF_END_DT, 
       J.CURRENT_IND,
       B.BUSINESS_UNIT INSTITUTION_CD,
       J.EMPL_STAT_CD,
       J.EMPL_STAT_LD,
       J.EMPL_TYPE_CD, 
       J.EMPL_TYPE_LD, 
       J.FULL_PT_CD, 
       J.FULL_PT_LD, 
       J.HR_STAT_CD, 
       J.HR_STAT_LD, 
       C.JOBCODE_ID, 
       C.JOBCODE_LD,
       J.REG_TEMP_CD, 
       J.REG_TEMP_LD,
       'N',
       'S',
       SYSDATE,
       SYSDATE,
       1234       
  from PS_D_PERSON P
  join EXT
    on P.PERSON_ID = EXT.EMPLID
   and EXT.EXT_ORDER = 1
  join JOB J
    on J.EMPLID = EXT.HR_EMPLID
   and JOB_EFFSEQ_ORDER = 1
  join HRMRT_OWNER.PS_D_JOBCODE C
    on J.JOBCODE_SID = C.JOBCODE_SID
  join HRMRT_OWNER.PS_D_BUSINESS_UNIT B
    on J.BU_SID = B.BU_SID
 order by J.EFF_START_DT desc
; 
strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_D_EMPLOYEE_HIST rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_D_EMPLOYEE_HIST',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_D_EMPLOYEE_HIST';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_D_EMPLOYEE_HIST enable constraint PK_UM_D_EMPLOYEE_HIST';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_D_EMPLOYEE_HIST');

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

END UM_D_EMPLOYEE_HIST_P;
/

CREATE OR REPLACE PROCEDURE             "UM_D_CLASS_INSTRCTR_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads mart table UM_D_CLASS_INSTRCTR
--
--V01 SMT-xxxx 02/15/2018,    James Doucette
--                              Converted from SQL
--V01 SMT-xxxx 01/16/2019,    Srikanth,Pabbu (changed from merge to turnc and load)
--V01.1 Case 2618  03/24/2020,   Modified CLASS_INSTRCTR_ORDER logic.                              
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_D_CLASS_INSTRCTR';
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

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_D_CLASS_INSTRCTR';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_D_CLASS_INSTRCTR');

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_D_CLASS_INSTRCTR disable constraint PK_UM_D_CLASS_INSTRCTR';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
strMessage01    := 'Truncating table CSMRT_OWNER.UM_D_CLASS_INSTRCTR';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_D_CLASS_INSTRCTR';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_D_CLASS_INSTRCTR';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_D_CLASS_INSTRCTR';				

insert /*+ append */ into CSMRT_OWNER.UM_D_CLASS_INSTRCTR
  with X as (  
select FIELDNAME, FIELDVALUE, EFFDT, SRC_SYS_ID, 
       XLATLONGNAME, XLATSHORTNAME, DATA_ORIGIN, 
       row_number() over (partition by FIELDNAME, FIELDVALUE, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1800') else EFFDT end) desc) X_ORDER
  from CSSTG_OWNER.PSXLATITEM
where DATA_ORIGIN <> 'D'),
       Q1 as (  
select /*+ parallel(8) inline */ CRSE_ID CRSE_CD, CRSE_OFFER_NBR CRSE_OFFER_NUM, STRM TERM_CD, SESSION_CODE SESSION_CD, CLASS_SECTION CLASS_SECTION_CD, CLASS_MTG_NBR CLASS_MTG_NUM, 
       INSTR_ASSIGN_SEQ INSTRCTR_ASGN_NUM, SRC_SYS_ID, 
       EMPLID, INSTR_ROLE, GRADE_RSTR_ACCESS, CONTACT_MINUTES, SCHED_PRINT_INSTR, INSTR_LOAD_FACTOR, AUTO_CALC_WRKLD AUTOCALC_WRKLD_FLG,  
       DATA_ORIGIN
  from CSSTG_OWNER.PS_CLASS_INSTR
where DATA_ORIGIN <> 'D'), 
       S as (
select /*+ parallel(8) inline */ C.CRSE_CD, C.CRSE_OFFER_NUM, C.TERM_CD, C.SESSION_CD, C.CLASS_SECTION_CD, 
       C.CLASS_MTG_NUM, nvl(Q1.INSTRCTR_ASGN_NUM,0) INSTRCTR_ASGN_NUM, C.SRC_SYS_ID, 
       C.CLASS_MTG_PAT_ORDER,
--       row_number() over (partition by C.CLASS_SID, C.CLASS_MTG_NUM, C.SRC_SYS_ID
--                              order by nvl(Q1.DATA_ORIGIN,'-') desc, decode(nvl(Q1.INSTR_ROLE,'-'),'PI',0,1), 
--                                       nvl(Q1.INSTRCTR_ASGN_NUM,0)) CLASS_INSTRCTR_ORDER,																											  
-- Case 2618, March 2020 --
       row_number() over (partition by C.CLASS_SID, C.CLASS_MTG_NUM, C.SRC_SYS_ID
                              order by nvl(Q1.DATA_ORIGIN,'-') desc, decode(nvl(Q1.INSTR_ROLE,'-'),'PI',0,'SI',1,'TA',2,3),  
							           nvl(Q1.INSTRCTR_ASGN_NUM,0)) CLASS_INSTRCTR_ORDER,
       C.INSTITUTION_CD, C.CLASS_SID, C.CLASS_MTG_PAT_SID,
       nvl(P.PERSON_SID, 2147483646) PERSON_SID, 
       nvl(R.INSTRCTR_ROLE_SID, 2147483646) INSTRCTR_ROLE_SID, 
       Q1.GRADE_RSTR_ACCESS, nvl(X1.XLATSHORTNAME,'') GRADE_RSTR_ACCESS_SD, nvl(X1.XLATLONGNAME,'') GRADE_RSTR_ACCESS_LD,
       Q1.CONTACT_MINUTES, Q1.SCHED_PRINT_INSTR, Q1.INSTR_LOAD_FACTOR, Q1.AUTOCALC_WRKLD_FLG,  
       least(C.DATA_ORIGIN,nvl(Q1.DATA_ORIGIN,'Z')) DATA_ORIGIN  
  from CSMRT_OWNER.UM_D_CLASS_MTG_PAT C 
  left outer join Q1   
    on C.CRSE_CD = Q1.CRSE_CD
   and C.CRSE_OFFER_NUM = Q1.CRSE_OFFER_NUM
   and C.TERM_CD = Q1.TERM_CD
   and C.SESSION_CD = Q1.SESSION_CD
   and C.CLASS_SECTION_CD = Q1.CLASS_SECTION_CD
   and C.CLASS_MTG_NUM = Q1.CLASS_MTG_NUM
   and C.SRC_SYS_ID = Q1.SRC_SYS_ID
  left outer join PS_D_PERSON P
    on Q1.EMPLID = P.PERSON_ID
   and Q1.SRC_SYS_ID = P.SRC_SYS_ID
   and P.DATA_ORIGIN <> 'D'
  left outer join PS_D_INSTRCTR_ROLE R
    on Q1.INSTR_ROLE = R.INSTRCTR_ROLE_CD
   and Q1.SRC_SYS_ID = R.SRC_SYS_ID
   and R.DATA_ORIGIN <> 'D'
  left outer join X X1
    on Q1.GRADE_RSTR_ACCESS = X1.FIELDVALUE
   and Q1.SRC_SYS_ID = X1.SRC_SYS_ID
   and X1.FIELDNAME = 'GRADE_RSTR_ACCESS'
   and X1.X_ORDER = 1
    )                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             
select /*+ parallel(8) */ ROWNUM CLASS_INSTRCTR_SID, 
       CRSE_CD, CRSE_OFFER_NUM, TERM_CD, SESSION_CD, CLASS_SECTION_CD, CLASS_MTG_NUM, INSTRCTR_ASGN_NUM, SRC_SYS_ID, 
       CLASS_MTG_PAT_ORDER, CLASS_INSTRCTR_ORDER, INSTITUTION_CD, CLASS_SID, CLASS_MTG_PAT_SID, PERSON_SID, INSTRCTR_ROLE_SID, 
       GRADE_RSTR_ACCESS, GRADE_RSTR_ACCESS_SD, GRADE_RSTR_ACCESS_LD, CONTACT_MINUTES, SCHED_PRINT_INSTR, INSTR_LOAD_FACTOR, AUTOCALC_WRKLD_FLG, 
       DATA_ORIGIN, SYSDATE CREATED_EW_DTTM, SYSDATE LASTUPD_EW_DTTM
  from S
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_D_CLASS_INSTRCTR rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_D_CLASS_INSTRCTR',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_D_CLASS_INSTRCTR';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_D_CLASS_INSTRCTR enable constraint PK_UM_D_CLASS_INSTRCTR';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_D_CLASS_INSTRCTR');

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

END UM_D_CLASS_INSTRCTR_P;
/

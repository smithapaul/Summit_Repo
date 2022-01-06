CREATE OR REPLACE PROCEDURE             "PS_DEPT_TBL_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_DEPT_TBL from PeopleSoft table PS_DEPT_TBL.
--
 --V01  SMT-xxxx 08/16/2017,    James Doucette
--                              Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_DEPT_TBL';
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
 where TABLE_NAME = 'PS_DEPT_TBL'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_DEPT_TBL@SASOURCE S)
 where TABLE_NAME = 'PS_DEPT_TBL'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_DEPT_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_DEPT_TBL';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_DEPT_TBL T 
using (select /*+ full(S) */
    nvl(trim(SETID),'-') SETID, 
    nvl(trim(DEPTID),'-') DEPTID, 
    EFFDT, 
    nvl(trim(EFF_STATUS),'-') EFF_STATUS, 
    nvl(trim(DESCR),'-') DESCR, 
    nvl(trim(DESCRSHORT),'-') DESCRSHORT, 
    nvl(trim(COMPANY),'-') COMPANY, 
    nvl(trim(SETID_LOCATION),'-') SETID_LOCATION, 
    nvl(trim(LOCATION),'-') LOCATION, 
    nvl(trim(TAX_LOCATION_CD),'-') TAX_LOCATION_CD, 
    nvl(trim(MANAGER_ID),'-') MANAGER_ID, 
    nvl(trim(MANAGER_POSN),'-') MANAGER_POSN, 
    nvl(BUDGET_YR_END_DT,0) BUDGET_YR_END_DT, 
    nvl(trim(BUDGET_LVL),'-') BUDGET_LVL, 
    nvl(trim(GL_EXPENSE),'-') GL_EXPENSE, 
    nvl(trim(EEO4_FUNCTION),'-') EEO4_FUNCTION, 
    nvl(trim(CAN_IND_SECTOR),'-') CAN_IND_SECTOR, 
    nvl(trim(ACCIDENT_INS),'-') ACCIDENT_INS, 
    nvl(trim(SI_ACCIDENT_NUM),'-') SI_ACCIDENT_NUM, 
    nvl(trim(HAZARD),'-') HAZARD, 
    nvl(trim(ESTABID),'-') ESTABID, 
    nvl(trim(RISKCD),'-') RISKCD, 
    nvl(trim(GVT_DESCR40),'-') GVT_DESCR40, 
    nvl(trim(GVT_SUB_AGENCY),'-') GVT_SUB_AGENCY, 
    nvl(trim(GVT_PAR_LINE2),'-') GVT_PAR_LINE2, 
    nvl(trim(GVT_PAR_LINE3),'-') GVT_PAR_LINE3, 
    nvl(trim(GVT_PAR_LINE4),'-') GVT_PAR_LINE4, 
    nvl(trim(GVT_PAR_LINE5),'-') GVT_PAR_LINE5, 
    nvl(trim(GVT_PAR_DESCR2),'-') GVT_PAR_DESCR2, 
    nvl(trim(GVT_PAR_DESCR3),'-') GVT_PAR_DESCR3, 
    nvl(trim(GVT_PAR_DESCR4),'-') GVT_PAR_DESCR4, 
    nvl(trim(GVT_PAR_DESCR5),'-') GVT_PAR_DESCR5, 
    nvl(trim(FTE_EDIT_INDC),'-') FTE_EDIT_INDC, 
    nvl(trim(DEPT_TENURE_FLG),'-') DEPT_TENURE_FLG, 
    nvl(trim(TL_DISTRIB_INFO),'-') TL_DISTRIB_INFO, 
    nvl(trim(USE_BUDGETS),'-') USE_BUDGETS, 
    nvl(trim(USE_ENCUMBRANCES),'-') USE_ENCUMBRANCES, 
    nvl(trim(USE_DISTRIBUTION),'-') USE_DISTRIBUTION, 
    nvl(trim(BUDGET_DEPTID),'-') BUDGET_DEPTID, 
    ' ' DIST_PRORATE_OPTN, 
    nvl(trim(HP_STATS_DEPT_CD),'-') HP_STATS_DEPT_CD, 
    nvl(trim(HP_STATS_FACULTY),'-') HP_STATS_FACULTY, 
    nvl(trim(MANAGER_NAME),'-') MANAGER_NAME, 
    nvl(trim(ACCOUNTING_OWNER),'-') ACCOUNTING_OWNER, 
    nvl(trim(COUNTRY_GRP),'-') COUNTRY_GRP, 
    nvl(trim(CLASS_UNIT_NZL),'-') CLASS_UNIT_NZL, 
    nvl(trim(ORG_UNIT_AUS),'-') ORG_UNIT_AUS, 
    nvl(trim(WORK_SECTOR_AUS),'-') WORK_SECTOR_AUS, 
    nvl(APS_AGENT_CD_AUS,0) APS_AGENT_CD_AUS, 
    nvl(trim(IND_COMMITTEE_BEL),'-') IND_COMMITTEE_BEL, 
    nvl(trim(NACE_CD_BEL),'-') NACE_CD_BEL, 
    ' ' BUDGETARY_ONLY, 
    0 SYNCID, 
    to_date(to_char('01/01/1900'), 'MM/DD/YYYY' )SYNCDTTM
from SYSADM.PS_DEPT_TBL@SASOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_DEPT_TBL') ) S 
 on ( 
    T.SETID = S.SETID and 
    T.DEPTID = S.DEPTID and 
    T.EFFDT = S.EFFDT and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.EFF_STATUS = S.EFF_STATUS,
    T.DESCR = S.DESCR,
    T.DESCRSHORT = S.DESCRSHORT,
    T.COMPANY = S.COMPANY,
    T.SETID_LOCATION = S.SETID_LOCATION,
    T.LOCATION = S.LOCATION,
    T.TAX_LOCATION_CD = S.TAX_LOCATION_CD,
    T.MANAGER_ID = S.MANAGER_ID,
    T.MANAGER_POSN = S.MANAGER_POSN,
    T.BUDGET_YR_END_DT = S.BUDGET_YR_END_DT,
    T.BUDGET_LVL = S.BUDGET_LVL,
    T.GL_EXPENSE = S.GL_EXPENSE,
    T.EEO4_FUNCTION = S.EEO4_FUNCTION,
    T.CAN_IND_SECTOR = S.CAN_IND_SECTOR,
    T.ACCIDENT_INS = S.ACCIDENT_INS,
    T.SI_ACCIDENT_NUM = S.SI_ACCIDENT_NUM,
    T.HAZARD = S.HAZARD,
    T.ESTABID = S.ESTABID,
    T.RISKCD = S.RISKCD,
    T.GVT_DESCR40 = S.GVT_DESCR40,
    T.GVT_SUB_AGENCY = S.GVT_SUB_AGENCY,
    T.GVT_PAR_LINE2 = S.GVT_PAR_LINE2,
    T.GVT_PAR_LINE3 = S.GVT_PAR_LINE3,
    T.GVT_PAR_LINE4 = S.GVT_PAR_LINE4,
    T.GVT_PAR_LINE5 = S.GVT_PAR_LINE5,
    T.GVT_PAR_DESCR2 = S.GVT_PAR_DESCR2,
    T.GVT_PAR_DESCR3 = S.GVT_PAR_DESCR3,
    T.GVT_PAR_DESCR4 = S.GVT_PAR_DESCR4,
    T.GVT_PAR_DESCR5 = S.GVT_PAR_DESCR5,
    T.FTE_EDIT_INDC = S.FTE_EDIT_INDC,
    T.DEPT_TENURE_FLG = S.DEPT_TENURE_FLG,
    T.TL_DISTRIB_INFO = S.TL_DISTRIB_INFO,
    T.USE_BUDGETS = S.USE_BUDGETS,
    T.USE_ENCUMBRANCES = S.USE_ENCUMBRANCES,
    T.USE_DISTRIBUTION = S.USE_DISTRIBUTION,
    T.BUDGET_DEPTID = S.BUDGET_DEPTID,
    T.DIST_PRORATE_OPTN = S.DIST_PRORATE_OPTN,
    T.HP_STATS_DEPT_CD = S.HP_STATS_DEPT_CD,
    T.HP_STATS_FACULTY = S.HP_STATS_FACULTY,
    T.MANAGER_NAME = S.MANAGER_NAME,
    T.ACCOUNTING_OWNER = S.ACCOUNTING_OWNER,
    T.COUNTRY_GRP = S.COUNTRY_GRP,
    T.CLASS_UNIT_NZL = S.CLASS_UNIT_NZL,
    T.ORG_UNIT_AUS = S.ORG_UNIT_AUS,
    T.WORK_SECTOR_AUS = S.WORK_SECTOR_AUS,
    T.APS_AGENT_CD_AUS = S.APS_AGENT_CD_AUS,
    T.IND_COMMITTEE_BEL = S.IND_COMMITTEE_BEL,
    T.NACE_CD_BEL = S.NACE_CD_BEL,
    T.BUDGETARY_ONLY = S.BUDGETARY_ONLY,
    T.SYNCID = S.SYNCID,
    T.SYNCDTTM = S.SYNCDTTM,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.EFF_STATUS <> S.EFF_STATUS or 
    T.DESCR <> S.DESCR or 
    T.DESCRSHORT <> S.DESCRSHORT or 
    T.COMPANY <> S.COMPANY or 
    T.SETID_LOCATION <> S.SETID_LOCATION or 
    T.LOCATION <> S.LOCATION or 
    T.TAX_LOCATION_CD <> S.TAX_LOCATION_CD or 
    T.MANAGER_ID <> S.MANAGER_ID or 
    T.MANAGER_POSN <> S.MANAGER_POSN or 
    T.BUDGET_YR_END_DT <> S.BUDGET_YR_END_DT or 
    T.BUDGET_LVL <> S.BUDGET_LVL or 
    T.GL_EXPENSE <> S.GL_EXPENSE or 
    T.EEO4_FUNCTION <> S.EEO4_FUNCTION or 
    T.CAN_IND_SECTOR <> S.CAN_IND_SECTOR or 
    T.ACCIDENT_INS <> S.ACCIDENT_INS or 
    T.SI_ACCIDENT_NUM <> S.SI_ACCIDENT_NUM or 
    T.HAZARD <> S.HAZARD or 
    T.ESTABID <> S.ESTABID or 
    T.RISKCD <> S.RISKCD or 
    T.GVT_DESCR40 <> S.GVT_DESCR40 or 
    T.GVT_SUB_AGENCY <> S.GVT_SUB_AGENCY or 
    T.GVT_PAR_LINE2 <> S.GVT_PAR_LINE2 or 
    T.GVT_PAR_LINE3 <> S.GVT_PAR_LINE3 or 
    T.GVT_PAR_LINE4 <> S.GVT_PAR_LINE4 or 
    T.GVT_PAR_LINE5 <> S.GVT_PAR_LINE5 or 
    T.GVT_PAR_DESCR2 <> S.GVT_PAR_DESCR2 or 
    T.GVT_PAR_DESCR3 <> S.GVT_PAR_DESCR3 or 
    T.GVT_PAR_DESCR4 <> S.GVT_PAR_DESCR4 or 
    T.GVT_PAR_DESCR5 <> S.GVT_PAR_DESCR5 or 
    T.FTE_EDIT_INDC <> S.FTE_EDIT_INDC or 
    T.DEPT_TENURE_FLG <> S.DEPT_TENURE_FLG or 
    T.TL_DISTRIB_INFO <> S.TL_DISTRIB_INFO or 
    T.USE_BUDGETS <> S.USE_BUDGETS or 
    T.USE_ENCUMBRANCES <> S.USE_ENCUMBRANCES or 
    T.USE_DISTRIBUTION <> S.USE_DISTRIBUTION or 
    T.BUDGET_DEPTID <> S.BUDGET_DEPTID or 
    T.DIST_PRORATE_OPTN <> S.DIST_PRORATE_OPTN or 
    T.HP_STATS_DEPT_CD <> S.HP_STATS_DEPT_CD or 
    T.HP_STATS_FACULTY <> S.HP_STATS_FACULTY or 
    T.MANAGER_NAME <> S.MANAGER_NAME or 
    T.ACCOUNTING_OWNER <> S.ACCOUNTING_OWNER or 
    T.COUNTRY_GRP <> S.COUNTRY_GRP or 
    T.CLASS_UNIT_NZL <> S.CLASS_UNIT_NZL or 
    T.ORG_UNIT_AUS <> S.ORG_UNIT_AUS or 
    T.WORK_SECTOR_AUS <> S.WORK_SECTOR_AUS or 
    T.APS_AGENT_CD_AUS <> S.APS_AGENT_CD_AUS or 
    T.IND_COMMITTEE_BEL <> S.IND_COMMITTEE_BEL or 
    T.NACE_CD_BEL <> S.NACE_CD_BEL or 
    T.BUDGETARY_ONLY <> S.BUDGETARY_ONLY or 
    T.SYNCID <> S.SYNCID or 
    nvl(trim(T.SYNCDTTM),0) <> nvl(trim(S.SYNCDTTM),0) or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.SETID,
    T.DEPTID, 
    T.EFFDT,
    T.SRC_SYS_ID, 
    T.EFF_STATUS, 
    T.DESCR,
    T.DESCRSHORT, 
    T.COMPANY,
    T.SETID_LOCATION, 
    T.LOCATION, 
    T.TAX_LOCATION_CD,
    T.MANAGER_ID, 
    T.MANAGER_POSN, 
    T.BUDGET_YR_END_DT, 
    T.BUDGET_LVL, 
    T.GL_EXPENSE, 
    T.EEO4_FUNCTION,
    T.CAN_IND_SECTOR, 
    T.ACCIDENT_INS, 
    T.SI_ACCIDENT_NUM,
    T.HAZARD, 
    T.ESTABID,
    T.RISKCD, 
    T.GVT_DESCR40,
    T.GVT_SUB_AGENCY, 
    T.GVT_PAR_LINE2,
    T.GVT_PAR_LINE3,
    T.GVT_PAR_LINE4,
    T.GVT_PAR_LINE5,
    T.GVT_PAR_DESCR2, 
    T.GVT_PAR_DESCR3, 
    T.GVT_PAR_DESCR4, 
    T.GVT_PAR_DESCR5, 
    T.FTE_EDIT_INDC,
    T.DEPT_TENURE_FLG,
    T.TL_DISTRIB_INFO,
    T.USE_BUDGETS,
    T.USE_ENCUMBRANCES, 
    T.USE_DISTRIBUTION, 
    T.BUDGET_DEPTID,
    T.DIST_PRORATE_OPTN,
    T.HP_STATS_DEPT_CD, 
    T.HP_STATS_FACULTY, 
    T.MANAGER_NAME, 
    T.ACCOUNTING_OWNER, 
    T.COUNTRY_GRP,
    T.CLASS_UNIT_NZL, 
    T.ORG_UNIT_AUS, 
    T.WORK_SECTOR_AUS,
    T.APS_AGENT_CD_AUS, 
    T.IND_COMMITTEE_BEL,
    T.NACE_CD_BEL,
    T.BUDGETARY_ONLY, 
    T.SYNCID, 
    T.SYNCDTTM, 
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
    ) 
values (
    S.SETID,
    S.DEPTID, 
    S.EFFDT,
    'CS90', 
    S.EFF_STATUS, 
    S.DESCR,
    S.DESCRSHORT, 
    S.COMPANY,
    S.SETID_LOCATION, 
    S.LOCATION, 
    S.TAX_LOCATION_CD,
    S.MANAGER_ID, 
    S.MANAGER_POSN, 
    S.BUDGET_YR_END_DT, 
    S.BUDGET_LVL, 
    S.GL_EXPENSE, 
    S.EEO4_FUNCTION,
    S.CAN_IND_SECTOR, 
    S.ACCIDENT_INS, 
    S.SI_ACCIDENT_NUM,
    S.HAZARD, 
    S.ESTABID,
    S.RISKCD, 
    S.GVT_DESCR40,
    S.GVT_SUB_AGENCY, 
    S.GVT_PAR_LINE2,
    S.GVT_PAR_LINE3,
    S.GVT_PAR_LINE4,
    S.GVT_PAR_LINE5,
    S.GVT_PAR_DESCR2, 
    S.GVT_PAR_DESCR3, 
    S.GVT_PAR_DESCR4, 
    S.GVT_PAR_DESCR5, 
    S.FTE_EDIT_INDC,
    S.DEPT_TENURE_FLG,
    S.TL_DISTRIB_INFO,
    S.USE_BUDGETS,
    S.USE_ENCUMBRANCES, 
    S.USE_DISTRIBUTION, 
    S.BUDGET_DEPTID,
    S.DIST_PRORATE_OPTN,
    S.HP_STATS_DEPT_CD, 
    S.HP_STATS_FACULTY, 
    S.MANAGER_NAME, 
    S.ACCOUNTING_OWNER, 
    S.COUNTRY_GRP,
    S.CLASS_UNIT_NZL, 
    S.ORG_UNIT_AUS, 
    S.WORK_SECTOR_AUS,
    S.APS_AGENT_CD_AUS, 
    S.IND_COMMITTEE_BEL,
    S.NACE_CD_BEL,
    S.BUDGETARY_ONLY, 
    S.SYNCID, 
    S.SYNCDTTM, 
    'N',
    'S',
    sysdate,
    sysdate,
    1234)
;

commit;


strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_DEPT_TBL rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_DEPT_TBL',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_DEPT_TBL';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_DEPT_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_DEPT_TBL';
update CSSTG_OWNER.PS_DEPT_TBL T
   set T.DATA_ORIGIN = 'D',
       T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select SETID, DEPTID, EFFDT
   from CSSTG_OWNER.PS_DEPT_TBL T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_DEPT_TBL') = 'Y'
  minus
 select SETID, DEPTID, EFFDT
   from SYSADM.PS_DEPT_TBL@SASOURCE S2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_DEPT_TBL') = 'Y'
   ) S
 where T.SETID = S.SETID
   and T.DEPTID = S.DEPTID
   and T.EFFDT = S.EFFDT
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_DEPT_TBL rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_DEPT_TBL',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_DEPT_TBL'
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

END PS_DEPT_TBL_P;
/

CREATE OR REPLACE PROCEDURE             "UM_D_PERSON_AGG_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
--George Adams
--Loads table                -- UM_D_PERSON_AGG
--V01 12/11/2018             -- srikanth ,pabbu converted to proc from sql scripts

------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_D_PERSON_AGG';
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

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_D_PERSON_AGG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_D_PERSON_AGG');

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_D_PERSON_AGG disable constraint PK_UM_D_PERSON_AGG';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
strMessage01    := 'Truncating table CSMRT_OWNER.UM_D_PERSON_AGG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_D_PERSON_AGG';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_D_PERSON_AGG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_D_PERSON_AGG';				
insert /*+ append */ into CSMRT_OWNER.UM_D_PERSON_AGG
  with Q1 as (
select /*+ inline parallel(8) */
       PERSON_SID, 
       max(case when CIT_ORDER = 1 then CITIZENSHIP_STATUS_USA else '-' end) CITZ_STAT_CD, 
       max(case when CIT_ORDER = 1 then CITIZENSHIP_STATUS_SD_USA else '-' end) CITZ_STAT_SD,
       max(case when CIT_ORDER = 1 then CITIZENSHIP_STATUS_LD_USA else '-' end) CITZ_STAT_LD, 
       max(case when CIT_ORDER = 1 and COUNTRY <> 'USA' and CITIZENSHIP_STATUS = '1' and CITIZENSHIP_STATUS_USA not in ('1','2') then COUNTRY else '-' end) CITZ_CNTRY_CD1, 
       max(case when CIT_ORDER = 1 and COUNTRY <> 'USA' and CITIZENSHIP_STATUS = '1' and CITIZENSHIP_STATUS_USA not in ('1','2') then COUNTRY_SD else '-' end) CITZ_CNTRY_SD1, 
       max(case when CIT_ORDER = 1 and COUNTRY <> 'USA' and CITIZENSHIP_STATUS = '1' and CITIZENSHIP_STATUS_USA not in ('1','2') then COUNTRY_LD else '-' end) CITZ_CNTRY_LD1, 
       max(case when CIT_ORDER = 1 and COUNTRY <> 'USA' and CITIZENSHIP_STATUS = '1' and CITIZENSHIP_STATUS_USA not in ('1','2') then COUNTRY_2CHAR else '-' end) CITZ_CNTRY_2CHAR1, 
       max(case when CIT_ORDER = 1 and COUNTRY <> 'USA' and CITIZENSHIP_STATUS = '1' and CITIZENSHIP_STATUS_USA not in ('1','2') then EU_MEMBER_STATE else '-' end) CITZ_CNTRY_EU_MEMBER1, 
       max(case when CIT_ORDER = 2 and COUNTRY <> 'USA' and CITIZENSHIP_STATUS = '1' and CITIZENSHIP_STATUS_USA not in ('1','2') then COUNTRY else '-' end) CITZ_CNTRY_CD2, 
       max(case when CIT_ORDER = 2 and COUNTRY <> 'USA' and CITIZENSHIP_STATUS = '1' and CITIZENSHIP_STATUS_USA not in ('1','2') then COUNTRY_SD else '-' end) CITZ_CNTRY_SD2, 
       max(case when CIT_ORDER = 2 and COUNTRY <> 'USA' and CITIZENSHIP_STATUS = '1' and CITIZENSHIP_STATUS_USA not in ('1','2') then COUNTRY_LD else '-' end) CITZ_CNTRY_LD2, 
       max(case when CIT_ORDER = 2 and COUNTRY <> 'USA' and CITIZENSHIP_STATUS = '1' and CITIZENSHIP_STATUS_USA not in ('1','2') then COUNTRY_2CHAR else '-' end) CITZ_CNTRY_2CHAR2, 
       max(case when CIT_ORDER = 2 and COUNTRY <> 'USA' and CITIZENSHIP_STATUS = '1' and CITIZENSHIP_STATUS_USA not in ('1','2') then EU_MEMBER_STATE else '-' end) CITZ_CNTRY_EU_MEMBER2, 
       max(case when CIT_ORDER = 3 and COUNTRY <> 'USA' and CITIZENSHIP_STATUS = '1' and CITIZENSHIP_STATUS_USA not in ('1','2') then COUNTRY else '-' end) CITZ_CNTRY_CD3, 
       max(case when CIT_ORDER = 3 and COUNTRY <> 'USA' and CITIZENSHIP_STATUS = '1' and CITIZENSHIP_STATUS_USA not in ('1','2') then COUNTRY_SD else '-' end) CITZ_CNTRY_SD3, 
       max(case when CIT_ORDER = 3 and COUNTRY <> 'USA' and CITIZENSHIP_STATUS = '1' and CITIZENSHIP_STATUS_USA not in ('1','2') then COUNTRY_LD else '-' end) CITZ_CNTRY_LD3, 
       max(case when CIT_ORDER = 3 and COUNTRY <> 'USA' and CITIZENSHIP_STATUS = '1' and CITIZENSHIP_STATUS_USA not in ('1','2') then COUNTRY_2CHAR else '-' end) CITZ_CNTRY_2CHAR3, 
       max(case when CIT_ORDER = 3 and COUNTRY <> 'USA' and CITIZENSHIP_STATUS = '1' and CITIZENSHIP_STATUS_USA not in ('1','2') then EU_MEMBER_STATE else '-' end) CITZ_CNTRY_EU_MEMBER3 
  from CSMRT_OWNER.UM_D_PERSON_CITIZEN  
 where DATA_ORIGIN <> 'D'
 group by PERSON_SID),
       Q2 as (
select /*+ inline parallel(8) */
       PERSON_SID, 
       max(case when AKA_ORDER = 1 and NAME_TYPE like 'AK1' then NAME else '' end) AK1_NAME,
       max(case when AKA_ORDER = 1 and NAME_TYPE like 'AK1' then FIRST_NAME else '' end) AK1_FIRST_NAME,
       max(case when AKA_ORDER = 1 and NAME_TYPE like 'AK1' then decode(trim(MIDDLE_NAME),'-','',MIDDLE_NAME) else '' end) AK1_MIDDLE_NAME,
       max(case when AKA_ORDER = 1 and NAME_TYPE like 'AK1' then LAST_NAME else '' end) AK1_LAST_NAME,
       max(case when AKA_ORDER = 1 and NAME_TYPE like 'AK1' then NAME_PREFIX else '' end) AK1_PREFIX,  
       max(case when AKA_ORDER = 1 and NAME_TYPE like 'AK1' then NAME_SUFFIX else '' end) AK1_SUFFIX,
       max(case when DEG_ORDER = 1 and NAME_TYPE = 'DEG' and EFF_STATUS <> 'I' then NAME else '' end) DEG_NAME,
       max(case when DEG_ORDER = 1 and NAME_TYPE = 'DEG' and EFF_STATUS <> 'I' then FIRST_NAME else '' end) DEG_FIRST_NAME,
       max(case when DEG_ORDER = 1 and NAME_TYPE = 'DEG' and EFF_STATUS <> 'I' then decode(trim(MIDDLE_NAME),'-','',MIDDLE_NAME) else '' end) DEG_MIDDLE_NAME,
       max(case when DEG_ORDER = 1 and NAME_TYPE = 'DEG' and EFF_STATUS <> 'I' then LAST_NAME else '' end) DEG_LAST_NAME,
       max(case when DEG_ORDER = 1 and NAME_TYPE = 'DEG' and EFF_STATUS <> 'I' then NAME_PREFIX else '' end) DEG_PREFIX,  
       max(case when DEG_ORDER = 1 and NAME_TYPE = 'DEG' and EFF_STATUS <> 'I' then NAME_SUFFIX else '' end) DEG_SUFFIX,
       max(case when PRF_ORDER = 1 and NAME_TYPE = 'PRF' then NAME else '' end) PRF_NAME,
       max(case when PRF_ORDER = 1 and NAME_TYPE = 'PRF' then FIRST_NAME else '' end) PRF_FIRST_NAME,
       max(case when PRF_ORDER = 1 and NAME_TYPE = 'PRF' then decode(trim(MIDDLE_NAME),'-','',MIDDLE_NAME) else '' end) PRF_MIDDLE_NAME,
       max(case when PRF_ORDER = 1 and NAME_TYPE = 'PRF' then LAST_NAME else '' end) PRF_LAST_NAME,
       max(case when PRF_ORDER = 1 and NAME_TYPE = 'PRF' then NAME_PREFIX else '' end) PRF_PREFIX,  
       max(case when PRF_ORDER = 1 and NAME_TYPE = 'PRF' then NAME_SUFFIX else '' end) PRF_SUFFIX,
       max(case when PRI_ORDER = 1 and NAME_TYPE = 'PRI' then NAME else '' end) PRI_NAME,
       max(case when PRI_ORDER = 1 and NAME_TYPE = 'PRI' then FIRST_NAME else '' end) PRI_FIRST_NAME,
       max(case when PRI_ORDER = 1 and NAME_TYPE = 'PRI' then decode(trim(MIDDLE_NAME),'-','',MIDDLE_NAME) else '' end) PRI_MIDDLE_NAME,
       max(case when PRI_ORDER = 1 and NAME_TYPE = 'PRI' then LAST_NAME else '' end) PRI_LAST_NAME,
       max(case when PRI_ORDER = 1 and NAME_TYPE = 'PRI' then NAME_PREFIX else '' end) PRI_PREFIX,  
       max(case when PRI_ORDER = 1 and NAME_TYPE = 'PRI' then NAME_SUFFIX else '' end) PRI_SUFFIX
  from CSMRT_OWNER.UM_D_PERSON_NAME 
 where DATA_ORIGIN <> 'D'
 group by PERSON_SID),
       Q4 as (
select /*+ inline parallel(8) */ 
       PERSON_SID, 
       substr(max(case when SRVC_IND_CD in ('SEV')  
                       then to_char(SRVC_IND_DTTM,'YYYYMMDDHH24MISS')||INSTITUTION_CD else '-' end),-5,5) INTER_STDNT_IND, 
       substr(max(case when INSTITUTION_CD = 'UMBOS' and SRVC_IND_CD in ('VET','DEP')  
                       then to_char(SRVC_IND_DTTM,'YYYYMMDDHH24MISS')||SRVC_IND_CD else '-' end),-3,3) SRVC_IND_CD_UMBOS, 
       substr(max(case when INSTITUTION_CD = 'UMDAR' and SRVC_IND_CD in ('VET','DEP')  
                       then to_char(SRVC_IND_DTTM,'YYYYMMDDHH24MISS')||SRVC_IND_CD else '-' end),-3,3) SRVC_IND_CD_UMDAR, 
       substr(max(case when INSTITUTION_CD = 'UMLOW' and SRVC_IND_CD in ('VET','DEP')  
                       then to_char(SRVC_IND_DTTM,'YYYYMMDDHH24MISS')||SRVC_IND_CD else '-' end),-3,3) SRVC_IND_CD_UMLOW 
  from UM_D_PERSON_SRVC_IND 
 where DATA_ORIGIN <> 'D'
   and trunc(SRVC_IND_DTTM) <= trunc(SYSDATE)
 group by PERSON_SID
 having substr(max(case when SRVC_IND_CD in ('SEV')  
                       then to_char(SRVC_IND_DTTM,'YYYYMMDDHH24MISS')||INSTITUTION_CD else '-' end),-5,5) is not NULL
     or substr(max(case when INSTITUTION_CD = 'UMBOS' and SRVC_IND_CD in ('VET','DEP')  
                       then to_char(SRVC_IND_DTTM,'YYYYMMDDHH24MISS')||SRVC_IND_CD else '-' end),-3,3) is not NULL
     or substr(max(case when INSTITUTION_CD = 'UMDAR' and SRVC_IND_CD in ('VET','DEP')  
                       then to_char(SRVC_IND_DTTM,'YYYYMMDDHH24MISS')||SRVC_IND_CD else '-' end),-3,3) is not NULL
     or substr(max(case when INSTITUTION_CD = 'UMLOW' and SRVC_IND_CD in ('VET','DEP')  
                       then to_char(SRVC_IND_DTTM,'YYYYMMDDHH24MISS')||SRVC_IND_CD else '-' end),-3,3) is not NULL),
       Q5 as (
select /*+ inline parallel(8) */ 
       PERSON_SID, 
       max(case when E_ADDR_TYPE = 'UBST' then EMAIL_ADDR else ('-') end) STDNT_EMAIL_UMBOS, 
       max(case when E_ADDR_TYPE = 'UDST' then EMAIL_ADDR else ('-') end) STDNT_EMAIL_UMDAR, 
       max(case when E_ADDR_TYPE = 'ULST' then EMAIL_ADDR else ('-') end) STDNT_EMAIL_UMLOW, 
       max(case when E_ADDR_TYPE = 'UBEM' then EMAIL_ADDR else ('-') end) EMPL_EMAIL_UMBOS, 
       max(case when E_ADDR_TYPE = 'UDEM' then EMAIL_ADDR else ('-') end) EMPL_EMAIL_UMDAR, 
       max(case when E_ADDR_TYPE = 'ULEM' then EMAIL_ADDR else ('-') end) EMPL_EMAIL_UMLOW, 
       max(case when E_ADDR_TYPE = 'PERS' then EMAIL_ADDR else ('-') end) PERS_EMAIL,
       max(case when E_ADDR_TYPE = 'BUSN' then EMAIL_ADDR else ('-') end) OTHER_EMAIL 
  from UM_D_PERSON_EMAIL  
 where DATA_ORIGIN <> 'D'
 group by PERSON_SID),
       Q6 as (
select /*+ inline parallel(8) */
       PERSON_SID, PHONE_TYPE, COUNTRY_CODE, PHONE, 
       case when PHONE_TYPE = 'CELL' and (row_number() over (partition by PERSON_SID, PHONE_TYPE
                                                                 order by PREF_PHONE_FLAG desc)) = 1
            then 1 
            else 0
        end CELL_ORDER, 
       case when PHONE_TYPE = 'PERM' and (row_number() over (partition by PERSON_SID, PHONE_TYPE
                                                                 order by PREF_PHONE_FLAG desc)) = 1
            then 1 
            else 0
        end PERM_ORDER, 
       case when PHONE_TYPE = 'BUS1' and (row_number() over (partition by PERSON_SID, PHONE_TYPE
                                                                 order by PREF_PHONE_FLAG desc)) = 1
            then 1 
            else 0
        end WORK_ORDER, 
       row_number() over (partition by PERSON_SID
                              order by PREF_PHONE_FLAG desc, decode(PHONE_TYPE,'PERM',0,'CELL',1,'BUS1',2,9),PHONE_TYPE) PREF_ORDER 
  from UM_D_PERSON_PHONE
 where DATA_ORIGIN <> 'D'),
       Q7 as (
select /*+ inline parallel(8) */
       PERSON_SID,  
       max(case when CELL_ORDER = 1 then PHONE        else '' end) CELL_PHONE_NUM,
       max(case when PERM_ORDER = 1 then PHONE        else '' end) HOME_PHONE_NUM,
       max(case when WORK_ORDER = 1 then PHONE        else '' end) WORK_PHONE_NUM,
       max(case when PREF_ORDER = 1 then PHONE_TYPE   else '' end) PREF_PHONE_TYPE,
       max(case when PREF_ORDER = 1 then COUNTRY_CODE else '' end) PREF_COUNTRY_CODE,
       max(case when PREF_ORDER = 1 then PHONE        else '' end) PREF_PHONE_NUM
  from Q6
 group by PERSON_SID),
--       Q8 as ( 
--select /*+ inline parallel(8) */ 
--       EMPLID, SRC_SYS_ID, EXTERNAL_SYSTEM_ID HR_PERSON_ID, 
--       row_number() over (partition by EMPLID, SRC_SYS_ID
--                              order by EFFDT desc) EXT_ORDER 
--  from CSSTG_OWNER.PS_EXTERNAL_SYSTEM 
-- where DATA_ORIGIN <> 'D' 
--   and EXTERNAL_SYSTEM = 'HR') 
       Q8 as ( 
select /*+ inline parallel(8) */ 
       EMPLID, EXTERNAL_SYSTEM, EFFDT, SRC_SYS_ID, 
       EXTERNAL_SYSTEM_ID,
       row_number() over (partition by EMPLID, EXTERNAL_SYSTEM, SRC_SYS_ID
                              order by EFFDT desc) EXT_ORDER 
  from CSSTG_OWNER.PS_EXTERNAL_SYSTEM 
 where DATA_ORIGIN <> 'D' 
   and EXTERNAL_SYSTEM in ('HR','IDM')) 
select /*+ inline parallel(8) */
       P.PERSON_SID, 
       P.PERSON_ID, P.SRC_SYS_ID, 
       nvl(Q2.PRI_NAME,'-') PERSON_NM, 
       nvl(Q2.PRI_FIRST_NAME,'-') FIRST_NM, 
       nvl(Q2.PRI_MIDDLE_NAME,'') MIDDLE_NM, 
       nvl(Q2.PRI_LAST_NAME,'-') LAST_NM, 
       nvl(Q2.PRI_PREFIX,'-') PREFIX, 
       nvl(Q2.PRI_SUFFIX,'-') SUFFIX, 
       nvl(Q2.PRF_NAME,'-') PREFERRED_NAME, 
       nvl(Q2.AK1_NAME,'-') AK1_NAME, 
       nvl(Q2.AK1_FIRST_NAME,'-') AK1_FIRST_NAME, 
       nvl(Q2.AK1_MIDDLE_NAME,'') AK1_MIDDLE_NAME, 
       nvl(Q2.AK1_LAST_NAME,'-') AK1_LAST_NAME, 
       nvl(Q2.AK1_PREFIX,'-') AK1_PREFIX, 
       nvl(Q2.AK1_SUFFIX,'-') AK1_SUFFIX, 
       case when Q2.DEG_NAME is NULL then nvl(Q2.PRI_NAME,'-') else nvl(Q2.DEG_NAME,'-') end DEG_NAME, 
       case when Q2.DEG_NAME is NULL then nvl(Q2.PRI_FIRST_NAME,'-') else nvl(Q2.DEG_FIRST_NAME,'-') end DEG_FIRST_NAME, 
       case when Q2.DEG_NAME is NULL then nvl(Q2.PRI_MIDDLE_NAME,'') else nvl(Q2.DEG_MIDDLE_NAME,'') end DEG_MIDDLE_NAME, 
       case when Q2.DEG_NAME is NULL then nvl(Q2.PRI_LAST_NAME,'-') else nvl(Q2.DEG_LAST_NAME,'-') end DEG_LAST_NAME, 
       case when Q2.DEG_NAME is NULL then nvl(Q2.PRI_PREFIX,'-') else nvl(Q2.DEG_PREFIX,'-') end DEG_PREFIX, 
       case when Q2.DEG_NAME is NULL then nvl(Q2.PRI_SUFFIX,'-') else nvl(Q2.DEG_SUFFIX,'-') end DEG_SUFFIX, 
       P.BIRTH_DT, P.BIRTH_PLACE, P.BIRTH_STATE, P.BIRTH_STATE_LD, P.BIRTH_COUNTRY, P.BIRTH_COUNTRY_SD, P.BIRTH_COUNTRY_LD, P.BIRTH_COUNTRY_2CHAR, P.BIRTH_COUNTRY_EU_MEMBER, 
       nvl(Q1.CITZ_STAT_CD,'-') CITZ_STAT_CD, nvl(Q1.CITZ_STAT_SD,'-') CITZ_STAT_SD, nvl(Q1.CITZ_STAT_LD,'-') CITZ_STAT_LD, 
       nvl(Q1.CITZ_CNTRY_CD1,'-') CITZ_CNTRY_CD1, nvl(Q1.CITZ_CNTRY_SD1,'-') CITZ_CNTRY_SD1, nvl(Q1.CITZ_CNTRY_LD1,'-') CITZ_CNTRY_LD1, 
       nvl(Q1.CITZ_CNTRY_2CHAR1,'-') CITZ_CNTRY_2CHAR1, nvl(Q1.CITZ_CNTRY_EU_MEMBER1,'-') CITZ_CNTRY_EU_MEMBER1, 
       nvl(Q1.CITZ_CNTRY_CD2,'-') CITZ_CNTRY_CD2, nvl(Q1.CITZ_CNTRY_SD2,'-') CITZ_CNTRY_SD2, nvl(Q1.CITZ_CNTRY_LD2,'-') CITZ_CNTRY_LD2, 
       nvl(Q1.CITZ_CNTRY_2CHAR2,'-') CITZ_CNTRY_2CHAR2, nvl(Q1.CITZ_CNTRY_EU_MEMBER2,'-') CITZ_CNTRY_EU_MEMBER2, 
       nvl(Q1.CITZ_CNTRY_CD3,'-') CITZ_CNTRY_CD3, nvl(Q1.CITZ_CNTRY_SD3,'-') CITZ_CNTRY_SD3, nvl(Q1.CITZ_CNTRY_LD3,'-') CITZ_CNTRY_LD3, 
       nvl(Q1.CITZ_CNTRY_2CHAR3,'-') CITZ_CNTRY_2CHAR3, nvl(Q1.CITZ_CNTRY_EU_MEMBER3,'-') CITZ_CNTRY_EU_MEMBER3, 
       P.DEATH_DT, P.DEATH_FLG, P.DEATH_PLACE, 
       nvl(P.EMERG_CNTCT_NM,'-') EMERG_CNTCT_NM, 
       nvl(P.EMERG_RELATIONSHIP,'-') EMERG_RELATIONSHIP, 
       nvl(P.EMERG_RELATIONSHIP_SD,'-') EMERG_RELATIONSHIP_SD, 
       nvl(P.EMERG_RELATIONSHIP_LD,'-') EMERG_RELATIONSHIP_LD, 
       nvl(P.EMERG_ADDR1_LD,'-') EMERG_ADDR1_LD, 
       nvl(P.EMERG_ADDR2_LD,'-') EMERG_ADDR2_LD, 
       nvl(P.EMERG_ADDR3_LD,'-') EMERG_ADDR3_LD, 
       nvl(P.EMERG_ADDR4_LD,'-') EMERG_ADDR4_LD, 
       nvl(P.EMERG_CITY_NM,'-') EMERG_CITY_NM, 
       nvl(P.EMERG_STATE_CD,'-') EMERG_STATE_CD, 
       nvl(P.EMERG_POSTAL_CD,'-') EMERG_POSTAL_CD, 
       nvl(P.EMERG_CNTRY_CD,'-') EMERG_CNTRY_CD, 
       nvl(P.EMERG_COUNTRY_CODE,'-') EMERG_COUNTRY_CODE, 
       nvl(P.EMERG_PHONE_NUM,'-') EMERG_PHONE_NUM, 
       nvl(E1.ETHNIC_GRP_SID,2147483646) ETHNIC_GRP_FED_SID, 
       nvl(E2.ETHNIC_GRP_SID,2147483646) ETHNIC_GRP_ST_SID, 
       P.FERPA_FLG, P.GENDER_CD, P.GENDER_SD, P.GENDER_LD, 
       nvl(P.HI_EDU_LVL_CD,'-') HI_EDU_LVL_CD, 
       nvl(P.HI_EDU_LVL_SD,'-') HI_EDU_LVL_SD, 
       nvl(P.HI_EDU_LVL_LD,'-') HI_EDU_LVL_LD, 
       Q4.INTER_STDNT_IND, 
       nvl(P.MAR_STAT_CD,'-') MAR_STAT_CD, 
       nvl(P.MAR_STAT_SD,'-') MAR_STAT_SD, 
       nvl(P.MAR_STAT_LD,'-') MAR_STAT_LD, 
       P.MAR_STAT_DT,  
       nvl(P.MIL_STAT_CD,'-') MIL_STAT_CD, 
       nvl(P.MIL_STAT_SD,'-') MIL_STAT_SD, 
       nvl(P.MIL_STAT_LD,'-') MIL_STAT_LD, 
       nvl(P.ITIN,'-') ITIN, 
       nvl(P.NTNL_ID,'-') NTNL_ID, 
       nvl(P.NTNL_ID_ERR_CHK,'-') NTNL_ID_ERR_CHK, 
       nvl(P.MASKED_NTNL_ID,'-') MASKED_NTNL_ID, 
       --OFF_RESIDENCY, OFF_RESIDENCY_DESC, OFF_RESIDENCY_DT, OFF_CITY, OFF_COUNTY, OFF_STATE, OFF_COUNTRY,     -- Retired!!!
       P.SEVIS_ID,
       P.UNDER_MINORITY_FLAG,   -- Feb 2020  
       --STDNT_CAMPUS_ID,   -- Retired!!!
       nvl(P.US_WORK_ELIG_IND,'-') US_WORK_ELIG_IND, 
       nvl(P.VA_BENEFIT,'-') VA_BENEFIT, 
       case when nvl(Q4.SRVC_IND_CD_UMBOS,'-') = 'DEP' 
            then 'Veteran Dependent - Verified'
            when nvl(Q4.SRVC_IND_CD_UMBOS,'-') = 'VET' 
            then 'Veteran Status - Verified'
            when P.VA_BENEFIT = 'Y' or P.MIL_STAT_CD IN ('3','4','5','6','7','8','9','A','C','E','G','M','O','S','T')
            then 'Veteran Status - Unverified'
            when P.MIL_STAT_CD IN ('D')
            then 'Veteran Dependent - Unverified'
            else 'Non-Veteran'
        end VETERAN_STATUS_UMBOS,  
       case when nvl(Q4.SRVC_IND_CD_UMDAR,'-') = 'DEP' 
            then 'Veteran Dependent - Verified'
            when nvl(Q4.SRVC_IND_CD_UMDAR,'-') = 'VET' 
            then 'Veteran Status - Verified'
            when P.VA_BENEFIT = 'Y' or P.MIL_STAT_CD IN ('3','4','5','6','7','8','9','A','C','E','G','M','O','S','T')
            then 'Veteran Status - Unverified'
            when P.MIL_STAT_CD IN ('D')
            then 'Veteran Dependent - Unverified'
            else 'Non-Veteran'
        end VETERAN_STATUS_UMDAR, 
       case when nvl(Q4.SRVC_IND_CD_UMLOW,'-') = 'DEP' 
            then 'Veteran Dependent - Verified'
            when nvl(Q4.SRVC_IND_CD_UMLOW,'-') = 'VET' 
            then 'Veteran Status - Verified'
--            when P.VA_BENEFIT = 'Y' or P.MIL_STAT_CD IN ('3','4','5','6','7','8','9','A','C','E','G','M','O','S','T')
            when P.VA_BENEFIT = 'Y' or P.MIL_STAT_CD IN ('3','4','5','6','7','8','9','A','C','E','G','M','O','S','T','R','Z')   -- April 2021 
            then 'Veteran Status - Unverified'
            when P.MIL_STAT_CD IN ('D')
            then 'Veteran Dependent - Unverified'
            else 'Non-Veteran'
        end VETERAN_STATUS_UMLOW, 
       V.VISA_PERMIT_TYPE, V.VISA_PERMIT_TYPE_LD VISA_PERMT_TY_DESC, nvl(V.STATUS_DT, to_date('01-JAN-1900')) VISA_EFFDT, V.VISA_WRKPMT_STATUS, V.VISA_WRKPMT_STATUS_SD, V.VISA_WRKPMT_STATUS_LD, 
       A1.ADDRESS1 HOME_ADDR1_LD, A1.ADDRESS2 HOME_ADDR2_LD, A1.ADDRESS3 HOME_ADDR3_LD, A1.ADDRESS4 HOME_ADDR4_LD, A1.CITY HOME_CITY_NM, A1.STATE HOME_STATE_CD, A1.POSTAL HOME_POSTAL_CD,  
       A1.COUNTRY HOME_CNTRY_CD, A1.COUNTRY_SD HOME_CNTRY_SD, A1.COUNTRY_LD HOME_CNTRY_LD, A1.COUNTRY_2CHAR HOME_CNTRY_2CHAR, A1.EU_MEMBER_STATE HOME_CNTRY_EU_MEMBER,  
       A1.UMLOW_GRAD_PROXIMITY HOME_UMLOW_GRAD_PROXIMITY, A1.UMLOW_UGRD_PROXIMITY HOME_UMLOW_UGRD_PROXIMITY, 
       nvl(A2.STATE,'-')  PERM_STATE_CD, 
       nvl(A2.COUNTRY,'-')  PERM_CNTRY_CD, 
       nvl(A2.COUNTRY_SD,'-')  PERM_CNTRY_SD, 
       nvl(A2.COUNTRY_LD,'-')  PERM_CNTRY_LD, 
       nvl(A2.COUNTRY_2CHAR,'-')  PERM_CNTRY_2CHAR, 
       nvl(A2.EU_MEMBER_STATE,'-')  PERM_CNTRY_EU_MEMBER,  
       nvl(Q5.STDNT_EMAIL_UMBOS,'-') STDNT_EMAIL_UMBOS, 
       nvl(Q5.STDNT_EMAIL_UMDAR,'-') STDNT_EMAIL_UMDAR, 
       nvl(Q5.STDNT_EMAIL_UMLOW,'-') STDNT_EMAIL_UMLOW, 
       nvl(Q5.EMPL_EMAIL_UMBOS,'-') EMPL_EMAIL_UMBOS, 
       nvl(Q5.EMPL_EMAIL_UMDAR,'-') EMPL_EMAIL_UMDAR, 
       nvl(Q5.EMPL_EMAIL_UMLOW,'-') EMPL_EMAIL_UMLOW, 
       nvl(Q5.PERS_EMAIL,'-') PERS_EMAIL, 
       nvl(Q5.OTHER_EMAIL,'-') OTHER_EMAIL, 
       nvl(Q7.HOME_PHONE_NUM,'-') HOME_PHONE_NUM, 
       nvl(Q7.CELL_PHONE_NUM,'-') CELL_PHONE_NUM, 
       nvl(Q7.WORK_PHONE_NUM,'-') WORK_PHONE_NUM, 
       nvl(Q7.PREF_PHONE_TYPE,'-') PREF_PHONE_TYPE, 
       nvl(Q7.PREF_COUNTRY_CODE,'-') PREF_COUNTRY_CODE, 
       nvl(Q7.PREF_PHONE_NUM,'-') PREF_PHONE_NUM, 
       nvl(HR.EXTERNAL_SYSTEM_ID,'-') HR_PERSON_ID, 
       nvl(IDM.EXTERNAL_SYSTEM_ID,'-') UM_GUID,   
       'S' DATA_ORIGIN, SYSDATE CREATED_EW_DTTM, SYSDATE LASTUPD_EW_DTTM
  from CSMRT_OWNER.PS_D_PERSON P  
  left outer join Q1 
    on P.PERSON_SID = Q1.PERSON_SID
  left outer join Q2  
    on P.PERSON_SID = Q2.PERSON_SID
  left outer join Q4 
    on P.PERSON_SID = Q4.PERSON_SID
  left outer join Q5 
    on P.PERSON_SID = Q5.PERSON_SID
  left outer join Q7 
    on P.PERSON_SID = Q7.PERSON_SID
  left outer join Q8 HR 
    on P.PERSON_ID = HR.EMPLID
   and P.SRC_SYS_ID = HR.SRC_SYS_ID
   and HR.EXTERNAL_SYSTEM = 'HR'
   and HR.EXT_ORDER = 1 
  left outer join Q8 IDM
    on P.PERSON_ID = IDM.EMPLID
   and P.SRC_SYS_ID = IDM.SRC_SYS_ID
   and IDM.EXTERNAL_SYSTEM = 'IDM'
   and IDM.EXT_ORDER = 1 
  left outer join PS_D_ETHNIC_GRP E1
    on P.ETHNIC_GRP_FED_CD = E1.ETHNIC_GRP_CD
   and P.SRC_SYS_ID = E1.SRC_SYS_ID
   and E1.SETID = 'USA'
   and E1.DATA_ORIGIN <> 'D'
  left outer join PS_D_ETHNIC_GRP E2
    on P.ETHNIC_GRP_ST_CD = E2.ETHNIC_GRP_CD
   and P.SRC_SYS_ID = E2.SRC_SYS_ID
   and E2.SETID = 'USA'
   and E2.DATA_ORIGIN <> 'D'
  left outer join UM_D_PERSON_VISA V
    on P.PERSON_SID = V.PERSON_SID
   and V.VISA_ORDER = 1
   and V.DATA_ORIGIN <> 'D'
  left outer join UM_D_PERSON_ADDR A1
    on P.PERSON_SID = A1.PERSON_SID
   and A1.PML_ADDR_ORDER = 1     -- Need RESH in _ORDER column?      
   and A1.DATA_ORIGIN <> 'D'
  left outer join UM_D_PERSON_ADDR A2
    on P.PERSON_SID = A2.PERSON_SID
   and A2.PERM_ADDR_ORDER = 1 
   and A2.ADDRESS_TYPE = 'PERM'
   and A2.EFF_STATUS <> 'I'
   and A2.DATA_ORIGIN <> 'D'
 where P.DATA_ORIGIN <> 'D'
   and not (upper(nvl(PRI_LAST_NAME,'-')) like 'X%DUP%'     -- Nov 2019 
        or  upper(nvl(PRI_LAST_NAME,'-')) like '%XXX%')     -- Nov 2019 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_D_PERSON_AGG rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_D_PERSON_AGG',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_D_PERSON_AGG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_D_PERSON_AGG enable constraint PK_UM_D_PERSON_AGG';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_D_PERSON_AGG');

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

END UM_D_PERSON_AGG_P;
/

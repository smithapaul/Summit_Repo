DROP PROCEDURE CSMRT_OWNER.AM_PS_LOCATION_TBL_P
/

--
-- AM_PS_LOCATION_TBL_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER.AM_PS_LOCATION_TBL_P IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_LOCATION_TBL from PeopleSoft table PS_LOCATION_TBL.
--
-- V01  SMT-xxxx 9/15/2017,    James Doucette
--                             Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'AM_PS_LOCATION_TBL';
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
 where TABLE_NAME = 'PS_LOCATION_TBL'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Truncating',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_LOCATION_TBL@AMSOURCE S)
 where TABLE_NAME = 'PS_LOCATION_TBL'
;

strSqlCommand := 'commit';
commit;


strSqlDynamic   := 'truncate table AMSTG_OWNER.PS_T_LOCATION_TBL';
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
 where TABLE_NAME = 'PS_LOCATION_TBL'
;

strSqlCommand := 'commit';
commit;


strSqlCommand := 'insert';
insert /*+ append */  into AMSTG_OWNER.PS_T_LOCATION_TBL
select /*+ full(S) */
    nvl(trim(SETID),'-') SETID, 
    nvl(trim(LOCATION),'-') LOCATION, 
    EFFDT, 
    nvl(trim(SAL_ADMIN_PLAN),'-') SAL_ADMIN_PLAN, 
    nvl(trim(EFF_STATUS),'-') EFF_STATUS, 
    nvl(trim(DESCR),'-') DESCR, 
    nvl(trim(DESCR_AC),'-') DESCR_AC, 
    nvl(trim(DESCRSHORT),'-') DESCRSHORT, 
    nvl(trim(BUILDING),'-') BUILDING, 
    nvl(trim(FLOOR),'-') FLOOR, 
    nvl(trim(SECTOR),'-') SECTOR, 
    nvl(trim(JURISDICTION),'-') JURISDICTION, 
    nvl(trim(ATTN_TO),'-') ATTN_TO, 
    nvl(trim(COUNTRY),'-') COUNTRY, 
    nvl(trim(ADDRESS1),'-') ADDRESS1, 
    nvl(trim(ADDRESS2),'-') ADDRESS2, 
    nvl(trim(ADDRESS3),'-') ADDRESS3, 
    nvl(trim(ADDRESS4),'-') ADDRESS4, 
    nvl(trim(CITY),'-') CITY, 
    nvl(trim(NUM1),'-') NUM1, 
    nvl(trim(NUM2),'-') NUM2, 
    nvl(trim(HOUSE_TYPE),'-') HOUSE_TYPE, 
    nvl(trim(ADDR_FIELD1),'-') ADDR_FIELD1, 
    nvl(trim(ADDR_FIELD2),'-') ADDR_FIELD2, 
    nvl(trim(ADDR_FIELD3),'-') ADDR_FIELD3, 
    nvl(trim(COUNTY),'-') COUNTY, 
    nvl(trim(STATE),'-') STATE, 
    nvl(trim(POSTAL),'-') POSTAL, 
    nvl(trim(GEO_CODE),'-') GEO_CODE, 
    nvl(trim(IN_CITY_LIMIT),'-') IN_CITY_LIMIT, 
    nvl(trim(COUNTRY_CODE),'-') COUNTRY_CODE, 
    nvl(trim(PHONE),'-') PHONE, 
    nvl(trim(EXTENSION),'-') EXTENSION, 
    nvl(trim(FAX),'-') FAX, 
    nvl(trim(SETID_SALARY),'-') SETID_SALARY, 
    nvl(trim(LANG_CD),'-') LANG_CD, 
    nvl(trim(REG_REGION),'-') REG_REGION, 
    nvl(trim(HOLIDAY_SCHEDULE),'-') HOLIDAY_SCHEDULE, 
    nvl(trim(LOCALITY),'-') LOCALITY, 
    nvl(trim(CAN_CMA),'-') CAN_CMA, 
    nvl(trim(CAN_OEE_AREACD),'-') CAN_OEE_AREACD, 
    nvl(trim(GEOLOC_CODE),'-') GEOLOC_CODE, 
    nvl(trim(OFFICE_TYPE),'-') OFFICE_TYPE, 
    nvl(trim(NCR_SW_CAN),'-') NCR_SW_CAN, 
    nvl(trim(TBS_OFFICE_CD_CAN),'-') TBS_OFFICE_CD_CAN, 
    nvl(trim(SPK_COMM_ID_GER),'-') SPK_COMM_ID_GER, 
    nvl(trim(TARIFF_AREA_GER),'-') TARIFF_AREA_GER, 
    nvl(trim(TARIFF_GER),'-') TARIFF_GER, 
    nvl(trim(INDUST_INSP_ID_GER),'-') INDUST_INSP_ID_GER, 
    nvl(trim(NI_REPORT_SW_UK),'-') NI_REPORT_SW_UK, 
    nvl(trim(GVT_GEOLOC_CD),'-') GVT_GEOLOC_CD, 
    nvl(trim(GVT_DESIG_AGENT),'-') GVT_DESIG_AGENT, 
    nvl(MATRICULA_NBR,0) MATRICULA_NBR, 
    nvl(trim(FON_ER_ID_MEX),'-') FON_ER_ID_MEX, 
    nvl(trim(FON_OFFICE_MEX),'-') FON_OFFICE_MEX, 
    nvl(trim(LOC_TAX_MEX),'-') LOC_TAX_MEX, 
    nvl(trim(LOC_TAX_SPCL_MEX),'-') LOC_TAX_SPCL_MEX, 
    nvl(trim(ESTABID),'-') ESTABID, 
    nvl(trim(LABEL_FORMAT_ID2),'-') LABEL_FORMAT_ID2, 
    nvl(trim(LABEL_FORMAT_ID3),'-') LABEL_FORMAT_ID3, 
    nvl(trim(USG_LBL_FORMAT_ID),'-') USG_LBL_FORMAT_ID, 
    nvl(trim(MESSAGE_TEXT2),'-') MESSAGE_TEXT2, 
    '-' WRKS_CNCL_ID_LCL, 
    '-' SOC_SEC_WRK_CTR, 
    to_char(substr(trim(COMMENTS_2000), 1, 4000)) COMMENTS_2000,
    to_number(ORA_ROWSCN) SRC_SCN
from SYSADM.PS_LOCATION_TBL@AMSOURCE S
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging'
 where TABLE_NAME = 'PS_LOCATION_TBL'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into AMSTG_OWNER.PS_LOCATION_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into AMSTG_OWNER.PS_LOCATION_TBL';
merge /*+ use_hash(S,T) */ into AMSTG_OWNER.PS_LOCATION_TBL T 
using (select /*+ full(S) */
    nvl(trim(SETID),'-') SETID, 
    nvl(trim(LOCATION),'-') LOCATION, 
    EFFDT, 
    nvl(trim(SAL_ADMIN_PLAN),'-') SAL_ADMIN_PLAN, 
    nvl(trim(EFF_STATUS),'-') EFF_STATUS, 
    nvl(trim(replace(DESCR, '  ', ' ')),'-') DESCR, 
    nvl(trim(DESCR_AC),'-') DESCR_AC, 
    nvl(trim(DESCRSHORT),'-') DESCRSHORT, 
    nvl(trim(BUILDING),'-') BUILDING, 
    nvl(trim(FLOOR),'-') FLOOR, 
    nvl(trim(SECTOR),'-') SECTOR, 
    nvl(trim(JURISDICTION),'-') JURISDICTION, 
    nvl(trim(ATTN_TO),'-') ATTN_TO, 
    nvl(trim(COUNTRY),'-') COUNTRY, 
    nvl(trim(ADDRESS1),'-') ADDRESS1, 
    nvl(trim(ADDRESS2),'-') ADDRESS2, 
    nvl(trim(ADDRESS3),'-') ADDRESS3, 
    nvl(trim(ADDRESS4),'-') ADDRESS4, 
    nvl(trim(CITY),'-') CITY, 
    nvl(trim(NUM1),'-') NUM1, 
    nvl(trim(NUM2),'-') NUM2, 
    nvl(trim(HOUSE_TYPE),'-') HOUSE_TYPE, 
    nvl(trim(ADDR_FIELD1),'-') ADDR_FIELD1, 
    nvl(trim(ADDR_FIELD2),'-') ADDR_FIELD2, 
    nvl(trim(ADDR_FIELD3),'-') ADDR_FIELD3, 
    nvl(trim(COUNTY),'-') COUNTY, 
    nvl(trim(STATE),'-') STATE, 
    nvl(trim(POSTAL),'-') POSTAL, 
    nvl(trim(GEO_CODE),'-') GEO_CODE, 
    nvl(trim(IN_CITY_LIMIT),'-') IN_CITY_LIMIT, 
    nvl(trim(COUNTRY_CODE),'-') COUNTRY_CODE, 
    nvl(trim(PHONE),'-') PHONE, 
    nvl(trim(EXTENSION),'-') EXTENSION, 
    nvl(trim(FAX),'-') FAX, 
    nvl(trim(SETID_SALARY),'-') SETID_SALARY, 
    nvl(trim(LANG_CD),'-') LANG_CD, 
    nvl(trim(REG_REGION),'-') REG_REGION, 
    nvl(trim(HOLIDAY_SCHEDULE),'-') HOLIDAY_SCHEDULE, 
    nvl(trim(LOCALITY),'-') LOCALITY, 
    nvl(trim(CAN_CMA),'-') CAN_CMA, 
    nvl(trim(CAN_OEE_AREACD),'-') CAN_OEE_AREACD, 
    nvl(trim(GEOLOC_CODE),'-') GEOLOC_CODE, 
    nvl(trim(OFFICE_TYPE),'-') OFFICE_TYPE, 
    nvl(trim(NCR_SW_CAN),'-') NCR_SW_CAN, 
    nvl(trim(TBS_OFFICE_CD_CAN),'-') TBS_OFFICE_CD_CAN, 
    nvl(trim(SPK_COMM_ID_GER),'-') SPK_COMM_ID_GER, 
    nvl(trim(TARIFF_AREA_GER),'-') TARIFF_AREA_GER, 
    nvl(trim(TARIFF_GER),'-') TARIFF_GER, 
    nvl(trim(INDUST_INSP_ID_GER),'-') INDUST_INSP_ID_GER, 
    nvl(trim(NI_REPORT_SW_UK),'-') NI_REPORT_SW_UK, 
    nvl(trim(GVT_GEOLOC_CD),'-') GVT_GEOLOC_CD, 
    nvl(trim(GVT_DESIG_AGENT),'-') GVT_DESIG_AGENT, 
    nvl(MATRICULA_NBR,0) MATRICULA_NBR, 
    nvl(trim(FON_ER_ID_MEX),'-') FON_ER_ID_MEX, 
    nvl(trim(FON_OFFICE_MEX),'-') FON_OFFICE_MEX, 
    nvl(trim(LOC_TAX_MEX),'-') LOC_TAX_MEX, 
    nvl(trim(LOC_TAX_SPCL_MEX),'-') LOC_TAX_SPCL_MEX, 
    nvl(trim(ESTABID),'-') ESTABID, 
    nvl(trim(LABEL_FORMAT_ID2),'-') LABEL_FORMAT_ID2, 
    nvl(trim(LABEL_FORMAT_ID3),'-') LABEL_FORMAT_ID3, 
    nvl(trim(USG_LBL_FORMAT_ID),'-') USG_LBL_FORMAT_ID, 
    nvl(trim(MESSAGE_TEXT2),'-') MESSAGE_TEXT2, 
    '-' WRKS_CNCL_ID_LCL, 
    '-' SOC_SEC_WRK_CTR, 
    to_char(substr(trim(COMMENTS_2000), 1, 4000)) COMMENTS_2000
from AMSTG_OWNER.PS_T_LOCATION_TBL S 
where SRC_SCN > (select OLD_MAX_SCN from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_LOCATION_TBL') ) S 
 on ( 
    T.SETID = S.SETID and 
    T.LOCATION = S.LOCATION and 
    T.EFFDT = S.EFFDT and 
    T.SRC_SYS_ID = 'CS90')
    when matched then update set
    T.SAL_ADMIN_PLAN = S.SAL_ADMIN_PLAN,
    T.EFF_STATUS = S.EFF_STATUS,
    T.DESCR = S.DESCR,
    T.DESCR_AC = S.DESCR_AC,
    T.DESCRSHORT = S.DESCRSHORT,
    T.BUILDING = S.BUILDING,
    T.FLOOR = S.FLOOR,
    T.SECTOR = S.SECTOR,
    T.JURISDICTION = S.JURISDICTION,
    T.ATTN_TO = S.ATTN_TO,
    T.COUNTRY = S.COUNTRY,
    T.ADDRESS1 = S.ADDRESS1,
    T.ADDRESS2 = S.ADDRESS2,
    T.ADDRESS3 = S.ADDRESS3,
    T.ADDRESS4 = S.ADDRESS4,
    T.CITY = S.CITY,
    T.NUM1 = S.NUM1,
    T.NUM2 = S.NUM2,
    T.HOUSE_TYPE = S.HOUSE_TYPE,
    T.ADDR_FIELD1 = S.ADDR_FIELD1,
    T.ADDR_FIELD2 = S.ADDR_FIELD2,
    T.ADDR_FIELD3 = S.ADDR_FIELD3,
    T.COUNTY = S.COUNTY,
    T.STATE = S.STATE,
    T.POSTAL = S.POSTAL,
    T.GEO_CODE = S.GEO_CODE,
    T.IN_CITY_LIMIT = S.IN_CITY_LIMIT,
    T.COUNTRY_CODE = S.COUNTRY_CODE,
    T.PHONE = S.PHONE,
    T.EXTENSION = S.EXTENSION,
    T.FAX = S.FAX,
    T.SETID_SALARY = S.SETID_SALARY,
    T.LANG_CD = S.LANG_CD,
    T.REG_REGION = S.REG_REGION,
    T.HOLIDAY_SCHEDULE = S.HOLIDAY_SCHEDULE,
    T.LOCALITY = S.LOCALITY,
    T.CAN_CMA = S.CAN_CMA,
    T.CAN_OEE_AREACD = S.CAN_OEE_AREACD,
    T.GEOLOC_CODE = S.GEOLOC_CODE,
    T.OFFICE_TYPE = S.OFFICE_TYPE,
    T.NCR_SW_CAN = S.NCR_SW_CAN,
    T.TBS_OFFICE_CD_CAN = S.TBS_OFFICE_CD_CAN,
    T.SPK_COMM_ID_GER = S.SPK_COMM_ID_GER,
    T.TARIFF_AREA_GER = S.TARIFF_AREA_GER,
    T.TARIFF_GER = S.TARIFF_GER,
    T.INDUST_INSP_ID_GER = S.INDUST_INSP_ID_GER,
    T.NI_REPORT_SW_UK = S.NI_REPORT_SW_UK,
    T.GVT_GEOLOC_CD = S.GVT_GEOLOC_CD,
    T.GVT_DESIG_AGENT = S.GVT_DESIG_AGENT,
    T.MATRICULA_NBR = S.MATRICULA_NBR,
    T.FON_ER_ID_MEX = S.FON_ER_ID_MEX,
    T.FON_OFFICE_MEX = S.FON_OFFICE_MEX,
    T.LOC_TAX_MEX = S.LOC_TAX_MEX,
    T.LOC_TAX_SPCL_MEX = S.LOC_TAX_SPCL_MEX,
    T.ESTABID = S.ESTABID,
    T.LABEL_FORMAT_ID2 = S.LABEL_FORMAT_ID2,
    T.LABEL_FORMAT_ID3 = S.LABEL_FORMAT_ID3,
    T.USG_LBL_FORMAT_ID = S.USG_LBL_FORMAT_ID,
    T.MESSAGE_TEXT2 = S.MESSAGE_TEXT2,
    T.WRKS_CNCL_ID_LCL = S.WRKS_CNCL_ID_LCL,
    T.SOC_SEC_WRK_CTR = S.SOC_SEC_WRK_CTR,
    T.COMMENTS_2000 = S.COMMENTS_2000,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.SAL_ADMIN_PLAN <> S.SAL_ADMIN_PLAN or 
    T.EFF_STATUS <> S.EFF_STATUS or 
    T.DESCR <> S.DESCR or 
    T.DESCR_AC <> S.DESCR_AC or 
    T.DESCRSHORT <> S.DESCRSHORT or 
    T.BUILDING <> S.BUILDING or 
    T.FLOOR <> S.FLOOR or 
    T.SECTOR <> S.SECTOR or 
    T.JURISDICTION <> S.JURISDICTION or 
    T.ATTN_TO <> S.ATTN_TO or 
    T.COUNTRY <> S.COUNTRY or 
    T.ADDRESS1 <> S.ADDRESS1 or 
    T.ADDRESS2 <> S.ADDRESS2 or 
    T.ADDRESS3 <> S.ADDRESS3 or 
    T.ADDRESS4 <> S.ADDRESS4 or 
    T.CITY <> S.CITY or 
    T.NUM1 <> S.NUM1 or 
    T.NUM2 <> S.NUM2 or 
    T.HOUSE_TYPE <> S.HOUSE_TYPE or 
    T.ADDR_FIELD1 <> S.ADDR_FIELD1 or 
    T.ADDR_FIELD2 <> S.ADDR_FIELD2 or 
    T.ADDR_FIELD3 <> S.ADDR_FIELD3 or 
    T.COUNTY <> S.COUNTY or 
    T.STATE <> S.STATE or 
    T.POSTAL <> S.POSTAL or 
    T.GEO_CODE <> S.GEO_CODE or 
    T.IN_CITY_LIMIT <> S.IN_CITY_LIMIT or 
    T.COUNTRY_CODE <> S.COUNTRY_CODE or 
    T.PHONE <> S.PHONE or 
    T.EXTENSION <> S.EXTENSION or 
    T.FAX <> S.FAX or 
    T.SETID_SALARY <> S.SETID_SALARY or 
    T.LANG_CD <> S.LANG_CD or 
    T.REG_REGION <> S.REG_REGION or 
    T.HOLIDAY_SCHEDULE <> S.HOLIDAY_SCHEDULE or 
    T.LOCALITY <> S.LOCALITY or 
    T.CAN_CMA <> S.CAN_CMA or 
    T.CAN_OEE_AREACD <> S.CAN_OEE_AREACD or 
    T.GEOLOC_CODE <> S.GEOLOC_CODE or 
    T.OFFICE_TYPE <> S.OFFICE_TYPE or 
    T.NCR_SW_CAN <> S.NCR_SW_CAN or 
    T.TBS_OFFICE_CD_CAN <> S.TBS_OFFICE_CD_CAN or 
    T.SPK_COMM_ID_GER <> S.SPK_COMM_ID_GER or 
    T.TARIFF_AREA_GER <> S.TARIFF_AREA_GER or 
    T.TARIFF_GER <> S.TARIFF_GER or 
    T.INDUST_INSP_ID_GER <> S.INDUST_INSP_ID_GER or 
    T.NI_REPORT_SW_UK <> S.NI_REPORT_SW_UK or 
    T.GVT_GEOLOC_CD <> S.GVT_GEOLOC_CD or 
    T.GVT_DESIG_AGENT <> S.GVT_DESIG_AGENT or 
    T.MATRICULA_NBR <> S.MATRICULA_NBR or 
    T.FON_ER_ID_MEX <> S.FON_ER_ID_MEX or 
    T.FON_OFFICE_MEX <> S.FON_OFFICE_MEX or 
    T.LOC_TAX_MEX <> S.LOC_TAX_MEX or 
    T.LOC_TAX_SPCL_MEX <> S.LOC_TAX_SPCL_MEX or 
    T.ESTABID <> S.ESTABID or 
    T.LABEL_FORMAT_ID2 <> S.LABEL_FORMAT_ID2 or 
    T.LABEL_FORMAT_ID3 <> S.LABEL_FORMAT_ID3 or 
    T.USG_LBL_FORMAT_ID <> S.USG_LBL_FORMAT_ID or 
    T.MESSAGE_TEXT2 <> S.MESSAGE_TEXT2 or 
    T.WRKS_CNCL_ID_LCL <> S.WRKS_CNCL_ID_LCL or 
    T.SOC_SEC_WRK_CTR <> S.SOC_SEC_WRK_CTR or 
    nvl(trim(T.COMMENTS_2000),0) <> nvl(trim(S.COMMENTS_2000),0) or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.SETID,
    T.LOCATION, 
    T.EFFDT,
    T.SRC_SYS_ID, 
    T.SAL_ADMIN_PLAN, 
    T.EFF_STATUS, 
    T.DESCR,
    T.DESCR_AC, 
    T.DESCRSHORT, 
    T.BUILDING, 
    T.FLOOR,
    T.SECTOR, 
    T.JURISDICTION, 
    T.ATTN_TO,
    T.COUNTRY,
    T.ADDRESS1, 
    T.ADDRESS2, 
    T.ADDRESS3, 
    T.ADDRESS4, 
    T.CITY, 
    T.NUM1, 
    T.NUM2, 
    T.HOUSE_TYPE, 
    T.ADDR_FIELD1,
    T.ADDR_FIELD2,
    T.ADDR_FIELD3,
    T.COUNTY, 
    T.STATE,
    T.POSTAL, 
    T.GEO_CODE, 
    T.IN_CITY_LIMIT,
    T.COUNTRY_CODE, 
    T.PHONE,
    T.EXTENSION,
    T.FAX,
    T.SETID_SALARY, 
    T.LANG_CD,
    T.REG_REGION, 
    T.HOLIDAY_SCHEDULE, 
    T.LOCALITY, 
    T.CAN_CMA,
    T.CAN_OEE_AREACD, 
    T.GEOLOC_CODE,
    T.OFFICE_TYPE,
    T.NCR_SW_CAN, 
    T.TBS_OFFICE_CD_CAN,
    T.SPK_COMM_ID_GER,
    T.TARIFF_AREA_GER,
    T.TARIFF_GER, 
    T.INDUST_INSP_ID_GER, 
    T.NI_REPORT_SW_UK,
    T.GVT_GEOLOC_CD,
    T.GVT_DESIG_AGENT,
    T.MATRICULA_NBR,
    T.FON_ER_ID_MEX,
    T.FON_OFFICE_MEX, 
    T.LOC_TAX_MEX,
    T.LOC_TAX_SPCL_MEX, 
    T.ESTABID,
    T.LABEL_FORMAT_ID2, 
    T.LABEL_FORMAT_ID3, 
    T.USG_LBL_FORMAT_ID,
    T.MESSAGE_TEXT2,
    T.WRKS_CNCL_ID_LCL, 
    T.SOC_SEC_WRK_CTR,
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID,
    T.COMMENTS_2000
    ) 
values (
    S.SETID,
    S.LOCATION, 
    S.EFFDT,
    'CS90', 
    S.SAL_ADMIN_PLAN, 
    S.EFF_STATUS, 
    S.DESCR,
    S.DESCR_AC, 
    S.DESCRSHORT, 
    S.BUILDING, 
    S.FLOOR,
    S.SECTOR, 
    S.JURISDICTION, 
    S.ATTN_TO,
    S.COUNTRY,
    S.ADDRESS1, 
    S.ADDRESS2, 
    S.ADDRESS3, 
    S.ADDRESS4, 
    S.CITY, 
    S.NUM1, 
    S.NUM2, 
    S.HOUSE_TYPE, 
    S.ADDR_FIELD1,
    S.ADDR_FIELD2,
    S.ADDR_FIELD3,
    S.COUNTY, 
    S.STATE,
    S.POSTAL, 
    S.GEO_CODE, 
    S.IN_CITY_LIMIT,
    S.COUNTRY_CODE, 
    S.PHONE,
    S.EXTENSION,
    S.FAX,
    S.SETID_SALARY, 
    S.LANG_CD,
    S.REG_REGION, 
    S.HOLIDAY_SCHEDULE, 
    S.LOCALITY, 
    S.CAN_CMA,
    S.CAN_OEE_AREACD, 
    S.GEOLOC_CODE,
    S.OFFICE_TYPE,
    S.NCR_SW_CAN, 
    S.TBS_OFFICE_CD_CAN,
    S.SPK_COMM_ID_GER,
    S.TARIFF_AREA_GER,
    S.TARIFF_GER, 
    S.INDUST_INSP_ID_GER, 
    S.NI_REPORT_SW_UK,
    S.GVT_GEOLOC_CD,
    S.GVT_DESIG_AGENT,
    S.MATRICULA_NBR,
    S.FON_ER_ID_MEX,
    S.FON_OFFICE_MEX, 
    S.LOC_TAX_MEX,
    S.LOC_TAX_SPCL_MEX, 
    S.ESTABID,
    S.LABEL_FORMAT_ID2, 
    S.LABEL_FORMAT_ID3, 
    S.USG_LBL_FORMAT_ID,
    S.MESSAGE_TEXT2,
    S.WRKS_CNCL_ID_LCL, 
    S.SOC_SEC_WRK_CTR,
    'N',
    'S',
    sysdate,
    sysdate,
    1234,
    S.COMMENTS_2000)
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;


strMessage01    := '# of PS_LOCATION_TBL rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_LOCATION_TBL',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_LOCATION_TBL';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on AMSTG_OWNER.PS_LOCATION_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on AMSTG_OWNER.PS_LOCATION_TBL';
update AMSTG_OWNER.PS_LOCATION_TBL T
   set T.DATA_ORIGIN = 'D',
          T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select SETID, LOCATION, EFFDT
   from AMSTG_OWNER.PS_LOCATION_TBL T2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_LOCATION_TBL') = 'Y'
  minus
 select SETID, LOCATION, EFFDT
   from SYSADM.PS_LOCATION_TBL@AMSOURCE S2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_LOCATION_TBL') = 'Y'
   ) S
 where T.SETID = S.SETID
   and T.LOCATION = S.LOCATION
   and T.EFFDT = S.EFFDT
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_LOCATION_TBL rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_LOCATION_TBL',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on AMSTG_OWNER.UM_STAGE_JOBS';

update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_LOCATION_TBL'
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

END AM_PS_LOCATION_TBL_P;
/

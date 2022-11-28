DROP VIEW CSSTG_OWNER.UMOL_INSTITUTION_ROLES_S2_VW
/

--
-- UMOL_INSTITUTION_ROLES_S2_VW  (View) 
--
CREATE OR REPLACE VIEW CSSTG_OWNER.UMOL_INSTITUTION_ROLES_S2_VW
BEQUEATH DEFINER
AS 
select BB_SOURCE,PK1,ROLE_NAME,DESCRIPTION,ROLE_ID,DATA_SRC_PK1,GUEST_IND,REMOVABLE_IND,SELF_SELECTABLE_IND,ROW_STATUS,DELETE_FLAG,INSERT_TIME,UPDATE_TIME from CSSTG_OWNER.UMOL_INSTITUTION_ROLES_S2@SMTPROD
/

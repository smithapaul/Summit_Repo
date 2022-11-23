DROP FUNCTION CSMRT_OWNER.ISNUMBER
/

--
-- ISNUMBER  (Function) 
--
CREATE OR REPLACE function CSMRT_OWNER.isnumber(p_inNumber varchar2) return number
as
 v_dummy number;
begin
 select to_number(p_inNumber) into v_dummy from dual;
 return 0;
exception
 when others then return 1;
end isnumber;
/

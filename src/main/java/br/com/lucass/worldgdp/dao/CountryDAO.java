package br.com.lucass.worldgdp.dao;

import br.com.lucass.worldgdp.dao.mapper.CountryRowMapper;
import br.com.lucass.worldgdp.model.Country;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@Setter
public class CountryDAO {
    @Autowired
    NamedParameterJdbcTemplate namedParameterJdbcTemplate;

    private static final int PAGE_SIZE = 20;

    private static final String SELECT_CLAUSE = "SELECT "
            + "t.Code, "
            + "t.Name, "
            + "t.Continent, "
            + "t.Region, "
            + "t.SurfaceArea, "
            + "t.IndepYear, "
            + "t.Population, "
            + "t.LifeExpectancy, "
            + "t.GNP, "
            + "t.GNPOld, "
            + "t.LocalName, "
            + "t.GovernmentForm, "
            + "t.HeadOfState, "
            + "t.Capital, "
            + "t.Code2 "
            + "FROM country as t "
            + "LEFT OUTER JOIN city as c ON (c.id = t.Capital) ";

    private static final String SEARCH_WHERE_CLAUSE = " AND (LOWER(t.Name) LIKE CONCAT('%', LOWER(:search), '%')) ";

    private static final String CONTINENT_WHERE_CLAUSE = " AND t.Continent = :continent ";

    private static final String REGION_WHERE_CLAUSE = " AND t.Region = :region ";

    private static final String PAGINATION_WHERE_CLAUSE = " ORDER BY t.Code LIMIT :limit OFFSET :offset ";

    public List<Country> getCountries(Map<String, Object> params) {
        int pageNo = 1;
        if (params.containsKey("pageNo")) {
            pageNo = Integer.parseInt(params.get("pageNo").toString());
        }
        Integer offset = (pageNo -1) * PAGE_SIZE;
        params.put("offset", offset);
        params.put("size", PAGE_SIZE);
        return namedParameterJdbcTemplate.query(SELECT_CLAUSE
                + " WHERE 1 = 1 "
                + (!StringUtils.isEmpty(params.get("search")) ? SEARCH_WHERE_CLAUSE : "")
                + (!StringUtils.isEmpty(params.get("continent")) ? CONTINENT_WHERE_CLAUSE : "")
                + (!StringUtils.isEmpty(params.get("region")) ? REGION_WHERE_CLAUSE : "")
                + PAGINATION_WHERE_CLAUSE,
                params, new CountryRowMapper());
    }

    public int getCountriesCount(Map<String, Object> params) {
        return namedParameterJdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM country t "
                    + (!StringUtils.isEmpty(params.get("search")) ? SEARCH_WHERE_CLAUSE : "")
                    + (!StringUtils.isEmpty(params.get("continent")) ? CONTINENT_WHERE_CLAUSE : "")
                    + (!StringUtils.isEmpty(params.get("region")) ? REGION_WHERE_CLAUSE : ""),
                params, Integer.class);
    }

    public Country getCountryDetail(String code) {
        Map<String, String> params = new HashMap<String, String>();
        params.put("code", code);
        return namedParameterJdbcTemplate.queryForObject(SELECT_CLAUSE + " WHERE t.Code = :code",
                params, new CountryRowMapper());
    }

    public void editCountryDetail(String code, Country country) {
        namedParameterJdbcTemplate.update("UPDATE country SET "
                + "name = :name, "
                + "localname = :localName, "
                + "capital = :capital, "
                + "continent = :continent, "
                + "region = :region, "
                + "HeadOfState = :headOfState, "
                + "GovernmentForm = :governmentForm, "
                + "IndepYear = :indepYear, "
                + "SurfaceArea = :surfaceArea, "
                + "population = :population, "
                + "LifeExpectancy = :lifeExpectancy "
                + "WHERE Code = :code ",
                getCountryAsMap(code, country));
    }

    private Map<String, Object> getCountryAsMap(String code, Country country) {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("name", country.getName());
        map.put("localName", country.getLocalName());
        map.put("capital", country.getCapital().getId());
        map.put("continent", country.getContinent());
        map.put("region", country.getRegion());
        map.put("headOfState", country.getHeadOfState());
        map.put("governmentForm", country.getGovernmentForm());
        map.put("indepYear", country.getIndepYear());
        map.put("surfaceArea", country.getSurfaceArea());
        map.put("population", country.getPopulation());
        map.put("lifeExpectancy", country.getLifeExpectancy());
        map.put("code", code);
        return map;
    }
}

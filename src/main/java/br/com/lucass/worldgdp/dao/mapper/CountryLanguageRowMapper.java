package br.com.lucass.worldgdp.dao.mapper;

import br.com.lucass.worldgdp.model.CountryLanguage;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class CountryLanguageRowMapper implements RowMapper<CountryLanguage> {

    public CountryLanguage mapRow(ResultSet rs, int i) throws SQLException {
        CountryLanguage countryLng = new CountryLanguage();
        countryLng.setCountryCode(rs.getString("countrycode"));
        countryLng.setIsOfficial(rs.getString("isofficial"));
        countryLng.setLanguage(rs.getString("language"));
        countryLng.setPercentage(rs.getDouble("percentage"));
        return countryLng;
    }
}

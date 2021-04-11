package io.lesible;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.lesible.model.User;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * <p> @date: 2021-04-08 13:45</p>
 *
 * @author 何嘉豪
 */
@Slf4j
public class JsonPrint {

    @Test
    public void print() throws Exception {
        List<User> users = new ArrayList<>();
        users.add(new User(123L, "Relic"));
        users.add(new User(234L, "Lesible"));
        ObjectMapper om = new ObjectMapper();
        String json = om.writeValueAsString(users);
        log.info("json: {}", json);
    }

    @Test
    public void currentTimeMillis() {
        long currentTimeMillis = System.currentTimeMillis();
        log.info("30s after: {}", currentTimeMillis + 30000);
    }

}

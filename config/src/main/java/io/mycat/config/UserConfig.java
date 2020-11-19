package io.mycat.config;

import io.mycat.util.JsonUtil;
import lombok.*;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@EqualsAndHashCode
public class UserConfig {
    private String username;
    private String password;
    private String ip = "127.0.0.1";
    private String transactionType = "xa";

    public static void main(String[] args) {
        String s = JsonUtil.toJson(new UserConfig());
        System.out.println(s);
    }
}
package io.mycat.config;

import com.alibaba.druid.util.StringUtils;
import io.mycat.util.Base64Utils;
import io.mycat.util.JsonUtil;
import io.mycat.util.RSAUtils;
import java.util.function.BiFunction;
import lombok.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@EqualsAndHashCode
public class UserConfig implements KVObject {
  private static final Logger LOGGER = LoggerFactory.getLogger(UserConfig.class);

  private String username;
  private String password;
  private String ip = null;
  private String transactionType = "proxy";
  private String dialect = "mysql";
  private String schema;
  private int isolation = 3;
  private String encryptType = "";
  private String encodeKey = "";

  public static void main(String[] args) {
    String s = JsonUtil.toJson(new UserConfig());
    System.out.println(s);
  }

  @Override
  public String keyName() {
    return username;
  }

  @Override
  public String path() {
    return "users";
  }

  @Override
  public String fileName() {
    return "user";
  }

  public String getPassword() {
    if (StringUtils.isEmpty(encryptType)) {
      return password;
    }
    String decodePassword =
        EncryptType.getByName(encryptType).getDecryptFunction().apply(password, getEncodeKey());
    if (StringUtils.isEmpty(decodePassword)) {
      throw new RuntimeException("用户密码无法正确解密");
    }
    return decodePassword;
  }

  @Getter
  enum EncryptType {
    NONE("", (encodePassword, key) -> encodePassword),
    RSA(
        "rsa",
        (encodePassword, key) -> {
          try {
            return RSAUtils.decrypt(encodePassword, key);
          } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
          }
          return null;
        });

    String name;
    BiFunction<String, String, String> decryptFunction;

    EncryptType(String name, BiFunction<String, String, String> decryptFunction) {
      this.name = name;
      this.decryptFunction = decryptFunction;
    }

    public static EncryptType getByName(String name) {
      for (EncryptType value : EncryptType.values()) {
        if (value.getName().equals(name)) {
          return value;
        }
      }
      return EncryptType.NONE;
    }
  }
}

package apache.kafkaapi.dto;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class MessageRequestDto {
    private String key;
    private Object message; // JSON 형태의 모든 메시지를 받을 수 있도록 Object 타입으로 설정
}

import groovy.transform.CompileStatic
import groovy.transform.ToString

import java.time.Instant
@CompileStatic
@ToString
class User {
    Integer id
    String naam
    Instant lastLogin = Instant.now()

    List<Vacature> vacatures=[]
}
class Vacature{
    Integer id
    String Bedrijf='dit is een test'
}

import com.jdotsoft.jarloader.JarClassLoader;

import lombok.SneakyThrows;

public class Launcher {
    @SneakyThrows
    public static void main(String[] args) {
        JarClassLoader loader = new JarClassLoader();
        loader.invokeMain("Main", args);
    }
}

package io.trino.plugin.teradata.export;

import io.airlift.log.Logger;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Loads and manages function mappings from YAML configuration.
 * 
 * Maps Trino function names to Teradata SQL templates.
 * Templates use $1, $2, $3... placeholders for arguments.
 */
public class FunctionMappingConfig {
    private static final Logger log = Logger.get(FunctionMappingConfig.class);
    
    private static final String DEFAULT_CONFIG = "teradata-functions.yaml";
    
    private final Map<String, String> mappings = new ConcurrentHashMap<>();
    private final Set<String> disabled = Collections.synchronizedSet(new HashSet<>());
    
    private FunctionMappingConfig() {}
    
    /**
     * Load function mappings from default resource and optional external file.
     */
    public static FunctionMappingConfig load(Optional<String> externalPath) {
        FunctionMappingConfig config = new FunctionMappingConfig();
        
        // Load default mappings from classpath
        config.loadFromClasspath();
        
        // Override with external file if specified
        externalPath.ifPresent(config::loadFromFile);
        
        log.info("Loaded %d function mappings, %d disabled functions", 
                config.mappings.size(), config.disabled.size());
        
        return config;
    }
    
    /**
     * Load default mappings from classpath resource.
     */
    private void loadFromClasspath() {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(DEFAULT_CONFIG)) {
            if (is != null) {
                parseYaml(is);
                log.info("Loaded default function mappings from classpath");
            } else {
                log.warn("Default function mappings file not found on classpath: %s", DEFAULT_CONFIG);
            }
        } catch (Exception e) {
            log.warn(e, "Failed to load default function mappings from classpath");
        }
    }
    
    /**
     * Load mappings from external file (overrides defaults).
     */
    private void loadFromFile(String path) {
        Path filePath = Paths.get(path);
        if (!Files.exists(filePath)) {
            log.info("External function mappings file not found: %s (using defaults)", path);
            return;
        }
        
        try (InputStream is = Files.newInputStream(filePath)) {
            parseYaml(is);
            log.info("Loaded function mappings from external file: %s", path);
        } catch (Exception e) {
            log.warn(e, "Failed to load function mappings from: %s", path);
        }
    }
    
    /**
     * Parse YAML configuration.
     */
    @SuppressWarnings("unchecked")
    private void parseYaml(InputStream is) {
        Yaml yaml = new Yaml();
        Map<String, Object> root = yaml.load(is);
        
        if (root == null) {
            return;
        }
        
        // Parse function mappings
        Object functionsObj = root.get("functions");
        if (functionsObj instanceof Map) {
            Map<String, String> funcs = (Map<String, String>) functionsObj;
            for (Map.Entry<String, String> entry : funcs.entrySet()) {
                if (entry.getKey() != null && entry.getValue() != null) {
                    String key = entry.getKey().toLowerCase();
                    mappings.put(key, entry.getValue());
                }
            }
        }
        
        // Parse disabled functions
        Object disabledObj = root.get("disabled");
        if (disabledObj instanceof java.util.List) {
            java.util.List<String> disabledList = (java.util.List<String>) disabledObj;
            for (String func : disabledList) {
                if (func != null) {
                    disabled.add(func.toLowerCase());
                }
            }
        }
    }
    
    /**
     * Get the Teradata template for a Trino function.
     * 
     * @param trinoFunction The Trino function name (without $ prefix)
     * @return Template string if mapping exists, empty otherwise
     */
    public Optional<String> getTemplate(String trinoFunction) {
        if (trinoFunction == null) {
            return Optional.empty();
        }
        String key = trinoFunction.toLowerCase();
        return Optional.ofNullable(mappings.get(key));
    }
    
    /**
     * Check if a function is disabled for pushdown.
     */
    public boolean isDisabled(String functionName) {
        if (functionName == null) {
            return false;
        }
        return disabled.contains(functionName.toLowerCase());
    }
    
    /**
     * Apply a template with the given arguments.
     * Replaces $1, $2, $3... with corresponding argument SQL strings.
     * 
     * @param template The template string (e.g., "SUBSTR($1, $2, $3)")
     * @param args The SQL strings for each argument
     * @return The resulting SQL string
     */
    public static String applyTemplate(String template, java.util.List<String> args) {
        if (template == null || args == null) {
            return template;
        }
        
        String result = template;
        for (int i = 0; i < args.size(); i++) {
            String placeholder = "$" + (i + 1);
            result = result.replace(placeholder, args.get(i));
        }
        
        return result;
    }
    
    /**
     * Get the number of loaded mappings.
     */
    public int getMappingCount() {
        return mappings.size();
    }
    
    /**
     * Get the number of disabled functions.
     */
    public int getDisabledCount() {
        return disabled.size();
    }
}

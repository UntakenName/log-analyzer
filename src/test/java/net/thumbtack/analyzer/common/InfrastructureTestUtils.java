package net.thumbtack.analyzer.common;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AssignableTypeFilter;

import java.util.Set;


public class InfrastructureTestUtils {

    public static Set<BeanDefinition> getBeansClassesNamesForSuperClass(Class beanClass) {
        ClassPathScanningCandidateComponentProvider provider = new ClassPathScanningCandidateComponentProvider(false);
        provider.addIncludeFilter(new AssignableTypeFilter(beanClass));
        return provider.findCandidateComponents("net.thumbtack.analyzer");
    }
}

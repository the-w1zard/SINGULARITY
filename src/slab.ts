import { slab, SlabServices } from './slab.types';

export function expose<T extends object>(
  instance: T,
  name: keyof SlabServices & string,
): void {
  (slab as any)[name] = new Proxy(instance, {
    get(target, key, receiver) {
      const value = Reflect.get(target, key, target);

      if (typeof value === 'function') {
        return value.bind(target);
      }

      return value;
    },

    set(target, key, value) {
      Reflect.set(target, key, value, target);
      return true;
    },

    has(target, key) {
      return Reflect.has(target, key);
    },
  });
}

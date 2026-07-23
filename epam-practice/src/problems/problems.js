/**
 * ============================================================================
 * QUESTION / PROBLEM STATEMENT:
 * Implement standard Vanilla JS Polyfills & Utility Functions for Frontend Technical Interviews:
 * 
 * 1. Debounce: Delay invoking `fn` until after `delay` ms of inactivity. Support `immediate` and `cancel()`.
 * 2. Throttle: Limit invoking `fn` to at most once per `limit` ms interval.
 * 3. Array.prototype.myMap: Polyfill `Array.prototype.map` handling sparse arrays & `thisArg`.
 * 4. Array.prototype.myFilter: Polyfill `Array.prototype.filter` handling sparse arrays & `thisArg`.
 * 5. Array.prototype.myReduce: Polyfill `Array.prototype.reduce` with initial value and empty array checks.
 * 6. Promise.all: Custom `myPromiseAll` handling an array of promises concurrently.
 * 7. Promise.allSettled: Custom `myPromiseAllSettled` returning `{ status, value|reason }`.
 * 8. Deep Clone: `deepClone` supporting circular references (WeakMap), Date, RegExp, Arrays & Objects.
 * 9. EventEmitter: Custom Event Bus class with `on`, `off`, `emit`, and `once`.
 * 10. Currying: `curry` utility transforming a multi-argument function into curried unary functions.
 * ============================================================================
 */

// ==========================================
// 1. Debounce Function
// ==========================================
/**
 * Creates a debounced function that delays invoking `fn` until after `delay` milliseconds
 * have elapsed since the last time the debounced function was invoked.
 */
export function debounce(fn, delay, immediate = false) {
  let timerId = null;

  function debounced(...args) {
    const context = this;
    const callNow = immediate && !timerId;

    if (timerId) {
      clearTimeout(timerId);
    }

    timerId = setTimeout(() => {
      timerId = null;
      if (!immediate) {
        fn.apply(context, args);
      }
    }, delay);

    if (callNow) {
      fn.apply(context, args);
    }
  }

  debounced.cancel = function () {
    if (timerId) {
      clearTimeout(timerId);
      timerId = null;
    }
  };

  return debounced;
}

// --- Test 1: Debounce ---
const debouncedLog = debounce((msg) => console.log("1. Debounce Test:", msg), 50);
debouncedLog("First call (ignored)");
debouncedLog("Second call (ignored)");
debouncedLog("Final call (executed)");


// ==========================================
// 2. Throttle Function
// ==========================================
/**
 * Creates a throttled function that only invokes `fn` at most once per every `limit` milliseconds.
 */
export function throttle(fn, limit) {
  let lastFunc = null;
  let lastRan = 0;

  return function (...args) {
    const context = this;
    const now = Date.now();

    if (!lastRan) {
      fn.apply(context, args);
      lastRan = now;
    } else {
      clearTimeout(lastFunc);
      lastFunc = setTimeout(
        () => {
          if (Date.now() - lastRan >= limit) {
            fn.apply(context, args);
            lastRan = Date.now();
          }
        },
        Math.max(limit - (now - lastRan), 0),
      );
    }
  };
}

// --- Test 2: Throttle ---
const throttledLog = throttle((msg) => console.log("2. Throttle Test:", msg), 100);
throttledLog("Initial throttle fire");
throttledLog("Throttled out (suppressed)");


// ==========================================
// 3. Array.prototype.myMap
// ==========================================
if (!Array.prototype.myMap) {
  Array.prototype.myMap = function (callback, thisArg) {
    if (this == null) {
      throw new TypeError("Array.prototype.myMap called on null or undefined");
    }
    if (typeof callback !== "function") {
      throw new TypeError(callback + " is not a function");
    }

    const O = Object(this);
    const len = O.length >>> 0;
    const result = new Array(len);

    for (let i = 0; i < len; i++) {
      if (i in O) {
        result[i] = callback.call(thisArg, O[i], i, O);
      }
    }

    return result;
  };
}

// --- Test 3: Array.prototype.myMap ---
const numbersMap = [1, 2, 3, 4];
console.log("3. myMap Test:", numbersMap.myMap((x) => x * 10)); // [10, 20, 30, 40]


// ==========================================
// 4. Array.prototype.myFilter
// ==========================================
if (!Array.prototype.myFilter) {
  Array.prototype.myFilter = function (callback, thisArg) {
    if (this == null) {
      throw new TypeError("Array.prototype.myFilter called on null or undefined");
    }
    if (typeof callback !== "function") {
      throw new TypeError(callback + " is not a function");
    }

    const O = Object(this);
    const len = O.length >>> 0;
    const result = [];

    for (let i = 0; i < len; i++) {
      if (i in O) {
        const val = O[i];
        if (callback.call(thisArg, val, i, O)) {
          result.push(val);
        }
      }
    }

    return result;
  };
}

// --- Test 4: Array.prototype.myFilter ---
const numbersFilter = [1, 2, 3, 4, 5, 6];
console.log("4. myFilter Test:", numbersFilter.myFilter((x) => x % 2 === 0)); // [2, 4, 6]


// ==========================================
// 5. Array.prototype.myReduce
// ==========================================
if (!Array.prototype.myReduce) {
  Array.prototype.myReduce = function (callback, initialValue) {
    if (this == null) {
      throw new TypeError("Array.prototype.myReduce called on null or undefined");
    }
    if (typeof callback !== "function") {
      throw new TypeError(callback + " is not a function");
    }

    const O = Object(this);
    const len = O.length >>> 0;
    let k = 0;
    let accumulator;

    if (arguments.length >= 2) {
      accumulator = initialValue;
    } else {
      while (k < len && !(k in O)) {
        k++;
      }
      if (k >= len) {
        throw new TypeError("Reduce of empty array with no initial value");
      }
      accumulator = O[k++];
    }

    for (; k < len; k++) {
      if (k in O) {
        accumulator = callback(accumulator, O[k], k, O);
      }
    }

    return accumulator;
  };
}

// --- Test 5: Array.prototype.myReduce ---
const numbersReduce = [10, 20, 30];
console.log("5. myReduce Test:", numbersReduce.myReduce((acc, x) => acc + x, 100)); // 160


// ==========================================
// 6. Promise.all Polyfill
// ==========================================
export function myPromiseAll(promises) {
  return new Promise((resolve, reject) => {
    if (!Array.isArray(promises)) {
      return reject(new TypeError("Argument must be an iterable array"));
    }

    const results = [];
    let completed = 0;
    const total = promises.length;

    if (total === 0) {
      return resolve(results);
    }

    promises.forEach((item, index) => {
      Promise.resolve(item)
        .then((value) => {
          results[index] = value;
          completed++;
          if (completed === total) {
            resolve(results);
          }
        })
        .catch((error) => {
          reject(error);
        });
    });
  });
}

// --- Test 6: Promise.all ---
myPromiseAll([Promise.resolve("A"), Promise.resolve("B"), 100])
  .then((res) => console.log("6. myPromiseAll Test:", res)); // ["A", "B", 100]


// ==========================================
// 7. Promise.allSettled Polyfill
// ==========================================
export function myPromiseAllSettled(promises) {
  return new Promise((resolve) => {
    if (!Array.isArray(promises)) {
      return resolve([]);
    }

    const results = [];
    let completed = 0;
    const total = promises.length;

    if (total === 0) {
      return resolve(results);
    }

    promises.forEach((item, index) => {
      Promise.resolve(item)
        .then((value) => {
          results[index] = { status: "fulfilled", value };
        })
        .catch((reason) => {
          results[index] = { status: "rejected", reason };
        })
        .finally(() => {
          completed++;
          if (completed === total) {
            resolve(results);
          }
        });
    });
  });
}

// --- Test 7: Promise.allSettled ---
myPromiseAllSettled([Promise.resolve("Success"), Promise.reject("Error Occurred")])
  .then((res) => console.log("7. myPromiseAllSettled Test:", res));


// ==========================================
// 8. Deep Clone (Handles Circular References & Special Types)
// ==========================================
export function deepClone(obj, hash = new WeakMap()) {
  if (Object(obj) !== obj) return obj; // Primitives
  if (obj instanceof Date) return new Date(obj);
  if (obj instanceof RegExp) return new RegExp(obj.source, obj.flags);
  if (hash.has(obj)) return hash.get(obj); // Circular ref check

  const clone = Array.isArray(obj) ? [] : Object.create(Object.getPrototypeOf(obj));
  hash.set(obj, clone);

  Reflect.ownKeys(obj).forEach((key) => {
    clone[key] =
      Object(obj[key]) === obj[key] && typeof obj[key] !== "function"
        ? deepClone(obj[key], hash)
        : obj[key];
  });

  return clone;
}

// --- Test 8: Deep Clone ---
const originalObj = { name: "Alice", details: { age: 25 }, created: new Date() };
originalObj.self = originalObj; // Circular reference
const clonedObj = deepClone(originalObj);
console.log("8. deepClone Test:", clonedObj.details !== originalObj.details && clonedObj.self === clonedObj); // true


// ==========================================
// 9. EventEmitter / EventBus
// ==========================================
export class EventEmitter {
  constructor() {
    this.events = new Map();
  }

  on(event, listener) {
    if (!this.events.has(event)) {
      this.events.set(event, []);
    }
    this.events.get(event).push(listener);
    return () => this.off(event, listener);
  }

  off(event, listener) {
    if (!this.events.has(event)) return;
    const listeners = this.events.get(event).filter((l) => l !== listener && l.original !== listener);
    this.events.set(event, listeners);
  }

  emit(event, ...args) {
    if (!this.events.has(event)) return false;
    this.events.get(event).forEach((listener) => listener.apply(this, args));
    return true;
  }

  once(event, listener) {
    const onceWrapper = (...args) => {
      this.off(event, onceWrapper);
      listener.apply(this, args);
    };
    onceWrapper.original = listener;
    this.on(event, onceWrapper);
  }
}

// --- Test 9: EventEmitter ---
const bus = new EventEmitter();
bus.on("message", (msg) => console.log("9. EventEmitter Test:", msg));
bus.emit("message", "Hello Event Bus!");


// ==========================================
// 10. Currying Utility
// ==========================================
export function curry(fn) {
  return function curried(...args) {
    if (args.length >= fn.length) {
      return fn.apply(this, args);
    }
    return function (...nextArgs) {
      return curried.apply(this, args.concat(nextArgs));
    };
  };
}

// --- Test 10: Currying ---
const multiplyThree = (a, b, c) => a * b * c;
const curriedMultiply = curry(multiplyThree);
console.log("10. Curry Test:", curriedMultiply(2)(3)(4)); // 24

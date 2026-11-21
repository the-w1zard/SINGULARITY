// sauron-transformer.ts
import * as ts from "@typescript/typescript6";
import { createHash } from "crypto";
import path from "path";

const DEBUG = true;

type HookSignatureMap = Record<string, number>;

const HOOK_SIGNATURE_MAP: HookSignatureMap = {
  useAtom: 3,
  useAtomY: 3,
  useBatchY: 2,
  useChannel: 3,
  useChannelY: 3,
  useData: 2,
  useDataY: 2,
  useDebounceY: 3,
  useDebounce: 3,
  useDebouncedCallbackY: 4,
  useDebouncedCallback: 4,
  useThrottleY: 3,
  useThrottle: 3,
  useThrottledCallbackY: 4,
  useThrottledCallback: 4,
  useEffectY: 4,
  useEffect: 4,
  useGesture: 2,
  useGestureY: 2,
  useMachineY: 2,
  useMachine: 2,
  useMediaY: 2,
  useMedia: 2,
  useMemoY: 4,
  useMemo: 4,
  useObserverY: 2,
  useObserver: 2,
  usePortalY: 2,
  usePortal: 2,
  usePresenceY: 3,
  usePresence: 3,
  useResourceY: 3,
  useResource: 3,

  useRouteY: 2,
  useRoute: 2,
  useNavigateY: 1,
  useNavigate: 1,
  useSearchParamsY: 1,
  useSearchParams: 1,
  useFetcherY: 1,
  useFetcher: 1,
  useDeferY: 2,
  useDefer: 2,
  useCookiesY: 1,
  useCookies: 1,
  useSessionY: 3,
  useSession: 3,
  useAuthY: 1,
  useAuth: 1,
  useBlockerY: 2,
  useBlocker: 2,
  usePromptY: 3,
  usePrompt: 3,
  useOutletContextY: 2,
  useOutletContext: 2,
  useRouteErrorY: 1,
  useRouteError: 1,

  useSyncY: 4,
  useSync: 4,
  useTaskY: 3,
  useTask: 3,
  useUndoY: 3,
  useUndo: 3,
  useWorkerY: 3,
  useWorker: 3,
  useY: 3,
  use: 3,

  useCronBoard: 1,
  useCronCondition: 2,
  useCronGraph: 1,
  useCronHistory: 2,
  useCronJob: 2,
  useCronMetrics: 1,
  useCronQueue: 1,
  useCronTimeline: 2,
  useDevClock: 1,

  useCSE: 1,
  useValhalla: 1,

  useJobDead: 2,
  useJobMetrics: 1,
  useJobQueue: 2,
  useJobTimeline: 2,
  useJobWorkers: 1,
  useJobResult: 2,
  useJobStatus: 2,

  useInitRetry: 1,

  useActionData: 1,
  useLoaderData: 1,
  useNavigationType: 1,
  useNavigation: 1,
  useRouteLoaderData: 2,

  useSauronOptimistic: 2,
  useSauronSelector: 3,
  useSauron: 2,

  kDebouncedSignal: 3,
  useDebouncedSignal: 3,
  kLazySignal: 3,
  useLazySignal: 3,
  kLive: 4,
  useLive: 4,
  kSignalBinding: 4,
  useSignalBinding: 4,
  kSignalInput: 2,
  useSignalInput: 2,
  kSignalEffect: 2,
  useSignalEffect: 2,
  kSignalRef: 2,
  useSignalRef: 2,
  useSignal: 2,
  useSignal2: 2,

  useAll: 2,
  useComputed: 2,
  useFetchPages: 4,
  useFetch: 4,
  useMutate: 4,
  useSelect: 4,

  useAnimation: 2,
  useSpring: 3,
  useTransform: 3,
  useViewTransition: 1,

  useBatch: 3,
  useCache: 2,
  useCache2: 2,
  useEventSource: 3,
  useIdempotent: 3,
  useLiveQuery: 3,
  useLiveSignal: 2,
  useMutation: 3,
  useParallelLoader: 3,
  useQuery: 2,
  useRequest: 3,
  useRetry: 3,
  useSSE: 4,
  useStream: 3,
  useWebSocket: 3,

  useBattery: 1,
  useBluetooth: 1,
  useBroadcast: 3,
  useCookie: 3,
  useFileSystem: 1,
  useGeolocation: 2,
  useHover: 1,
  useLocalStorage: 4,
  useNetwork: 1,
  useNFC: 1,
  useOnline: 1,
  useOrientation: 1,
  useSessionStorage: 3,
  useShare: 1,
  useSpeechSynthesis: 1,
  useWakeLock: 1,

  useCallback: 3,
  useContext: 2,
  useReducer: 3,
  useImperativeHandle: 3,
  useDebugValue: 2,
  useDeferredValue: 2,
  useDurable: 3,
  useSyncExternalStore: 3,
  useACME: 2,
  useAlpine: 3,
  useAutoImport: 2,
  useBatch2: 3,
  useBatch3: 4,
  useBattleTested: 3,
  useBeforeUnload: 3,
  useBoost: 2,
  useBranch: 2,
  useCapability: 1,
  useClickOutsideMulti: 4,
  useClient: 1,
  useClipboard: 1,
  useCodeMigration: 3,
  useComputedMobX: 3,
  useControllableState: 2,
  useCounter: 3,
  useDarkMode: 2,
  useAsyncDebounce: 3,
  useDelegatedAnchors: 2,
  useDerived: 2,
  useDynamicList: 2,
  useEffectEvent: 2,
  useEffectEventHook: 2,
  useEffectEventWithDeps: 3,
  useEscapeKeydown: 3,
  useEither: 2,
  useEscapeKeydownDefault: 2,
  useEscapeKeydownWithElement: 3,
  useEscapeKeydownWithPriority: 4,
  useEscapeKeydownConditional: 4,
  useEventBus: 2,
  useEventListener: 4,
  useEvent: 5,
  useFavicon: 2,
  useFlux: 3,
  useFocusTrap: 2,
  useFocus: 3,
  useFragment: 3,
  useHistory: 2,
  useHref: 3,
  useHX: 4,
  useHydrated: 1,
  useId: 1,
  useIdempotentPayment: 2,
  useIdleCallback: 3,
  useImmer: 2,
  useInfiniteScroll: 3,
  useInfinite: 2,
  useInsertionEffect: 3,
  useIntent: 3,
  useIntersection: 2,
  useInterval: 4,
  useInvalidate: 1,
  useIO: 2,
  useIsHydrated: 1,
  useIsHydratedSync: 1,
  useIsHydratedWithDelay: 2,
  useIsHydratedRef: 1,
  useIsomorphicEffect: 3,
  useKeyPress: 4,
  useKeyboard: 4,
  useLayoutEffect: 3,
  useLazyMemo: 3,
  useLazy: 3,
  useLazyModule: 3,
  useLazyFetch: 3,
  useList: 2,
  useLocales: 1,
  useLocation: 1,
  useLongPress: 3,
  useMap: 2,
  useMediaQuery: 2,
  useMiddleware: 2,
  useMigrate: 3,
  useModel: 3,
  useOptimistic: 3,
  usePrevious: 2,
  useRef: 2,
  useRef1: 2,
  useRef2: 2,
  useSauronContext: 2,
  useSauronMulti: 2,
  useSSEStore: 1,
  useSSEValue: 2,
  useYmir: 3,
  useState: 2,
  useTransition: 1,
};

function shouldSkipFile(filePath: string): boolean {
  const normalized = filePath.replace(/\\/g, "/");

  return (
    normalized.includes("/node_modules/") ||
    normalized.includes("/.pnpm/") ||
    normalized.includes("/dist/") ||
    normalized.includes("/archive/") ||
    normalized.includes("/.vscode/") ||
    normalized.endsWith(".d.ts")
  );
}

function getHookName(node: ts.CallExpression): string | undefined {
  if (ts.isIdentifier(node.expression)) {
    const name = node.expression.text;

    return /^use[A-Z]/.test(name) ? name : undefined;
  }

  if (
    ts.isPropertyAccessExpression(node.expression) &&
    ts.isIdentifier(node.expression.expression) &&
    ts.isIdentifier(node.expression.name)
  ) {
    const name = node.expression.name.text;

    return /^use[A-Z]/.test(name) ? name : undefined;
  }

  return undefined;
}

function isAlreadyTagged(node: ts.CallExpression): boolean {
  const lastArgument = node.arguments.at(-1);

  return (
    !!lastArgument &&
    ts.isStringLiteral(lastArgument) &&
    lastArgument.text.startsWith("__sauron_")
  );
}

export default function sauronTransformer(
  program: ts.Program,
  config: { debug?: boolean } = {}
): ts.TransformerFactory<ts.SourceFile> {
  const debug = config.debug ?? DEBUG;

  return (context: ts.TransformationContext) => {
    const visitor = (node: ts.Node): ts.Node => {
      if (ts.isCallExpression(node)) {
        const hookName = getHookName(node);

        if (
          hookName &&
          !isAlreadyTagged(node) &&
          !shouldSkipFile(node.getSourceFile().fileName)
        ) {
          const sourceFile = node.getSourceFile();
          const filePath = path.resolve(sourceFile.fileName);
          const line = sourceFile.getLineAndCharacterOfPosition(
            node.getStart(sourceFile)
          ).line + 1;

          const baseHash = createHash("sha1")
            .update(`${filePath}:${hookName}:${line}`)
            .digest("hex")
            .slice(0, 8);

          const id = `__sauron_${hookName}_${baseHash}__`;
          const targetIndex = HOOK_SIGNATURE_MAP[hookName];
          const currentArguments = Array.from(node.arguments);

          let newArguments: ts.Expression[];

          if (targetIndex === undefined) {
            newArguments = [
              ...currentArguments,
              ts.factory.createStringLiteral(id),
            ];
          } else if (currentArguments.length <= targetIndex) {
            const missing = targetIndex - currentArguments.length;
            const fillers = Array.from(
              { length: missing },
              () =>
                ts.factory.createIdentifier("undefined") as ts.Expression
            );

            newArguments = [
              ...currentArguments,
              ...fillers,
              ts.factory.createStringLiteral(id),
            ];
          } else {
            newArguments = currentArguments;
          }

          if (debug) {
            console.log(
              `[sauron] ${path.basename(filePath)}:${line} ${hookName} -> ${id}`
            );
          }

          return ts.factory.updateCallExpression(
            node,
            node.expression,
            node.typeArguments,
            newArguments
          );
        }
      }

      return ts.visitEachChild(node, visitor, context);
    };

    return (sourceFile: ts.SourceFile) =>
      ts.visitNode(sourceFile, visitor, ts.isSourceFile);
  };
}

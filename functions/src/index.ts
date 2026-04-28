import * as admin from "firebase-admin";
import {
  Change,
  EventContext,
  database,
} from "firebase-functions/v1";
import { DataSnapshot } from "firebase-functions/v1/database";

admin.initializeApp();

type SeleccionOferta = {
  conductorId: string;
  createdAt?: number | object;
};

type PushRole = "clientes" | "conductores";

type PushPayload = {
  type: string;
  viajeId: string;
  targetRole: "cliente" | "conductor";
  title: string;
  body: string;
  clickAction: string;
  channelId: string;
};

type ViajeMap = {
  clienteId?: string;
  viajeActualConductorId?: string;
  estatus?: string;
  canceladoPor?: string | null;
};

const db = admin.database();

const ORDER = [
  "solicitud",
  "en_camino",
  "llego",
  "en_curso",
  "completado",
] as const;

function asMap(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object") {
    return {};
  }
  return value as Record<string, unknown>;
}

function asTrimmedString(value: unknown): string {
  return (value ?? "").toString().trim();
}

function canMove(from: string | null, to: string | null): boolean {
  if (!to) {
    return false;
  }

  if (from == null || from.trim().length === 0) {
    return to === "solicitud";
  }

  if (to === "cancelado") {
    return true;
  }

  if (from === "en_curso" && to === "completado_sin_pago") {
    return true;
  }

  const iFrom = ORDER.indexOf(from as (typeof ORDER)[number]);
  const iTo = ORDER.indexOf(to as (typeof ORDER)[number]);

  return iFrom >= 0 && iTo === iFrom + 1;
}

function buildWeeklyKey(viaje: Record<string, any>): string {
  const zonaId = (viaje.zonaId ?? '').toString().trim();
  const idxSemana = (viaje.idxSemana ?? '').toString().trim();

  if (!zonaId || !idxSemana) return '';

  return `${zonaId}${idxSemana}`;
}

function buildCompletedPayload(viaje: Record<string, any>) {
  return {
    comisionConductor: Number(viaje?.cotizado?.totalConductor ?? 0),
    costoCliente: Number(viaje?.cotizado?.totalCliente ?? 0),
    destinoDireccion: (viaje?.direcciones?.destino?.direccion ?? '').toString(),
    estatusViaje: (viaje?.estatus ?? '').toString(),
    fechaActualizado: admin.database.ServerValue.TIMESTAMP,
    metodoPago: (viaje?.metodoPago ?? '').toString(),
    origenDireccion: (viaje?.direcciones?.origen?.direccion ?? '').toString(),
    tipoServicioNombre: (viaje?.tarifaNombre ?? '').toString(),
  };
}

function buildCancelledPayload(viaje: Record<string, any>) {
  return {
    costoCliente: Number(viaje?.cotizado?.totalCliente ?? 0),
    destinoDireccion: (viaje?.direcciones?.destino?.direccion ?? '').toString(),
    fechaActualizado: admin.database.ServerValue.TIMESTAMP,
    metodoPago: (viaje?.metodoPago ?? '').toString(),
    origenDireccion: (viaje?.direcciones?.origen?.direccion ?? '').toString(),
    tipoServicioNombre: (viaje?.tarifaNombre ?? '').toString(),
  };
}

export const syncWeeklyAdminTrips = database
  .ref('TAXI_Viaje/{viajeId}/estatus')
  .onWrite(async (change, context) => {
    const before = (change.before.val() ?? '').toString().toLowerCase();
    const after = (change.after.val() ?? '').toString().toLowerCase();

    // No cambio real
    if (before === after) return null;

    // Solo nos interesan estos estados finales
    if (
      after !== 'completado' &&
      after !== 'completado_sin_pago' &&
      after !== 'cancelado'
    ) {
      return null;
    }

    const viajeId = context.params.viajeId;

    const snap = await db.ref(`TAXI_Viaje/${viajeId}`).get();
    if (!snap.exists()) return null;

    const viaje = snap.val() as Record<string, any>;

    const weeklyKey = buildWeeklyKey(viaje);

    if (!weeklyKey) {
      console.error('❌ weeklyKey inválido', { viajeId, viaje });
      return null;
    }

    const completedPath =
      `ViajesAdministracionCompletadoSemana/${weeklyKey}/viajes/${viajeId}`;

    const cancelledPath =
      `ViajesAdministracionCanceladoSemana/${weeklyKey}/viajes/${viajeId}`;

    const updates: Record<string, any> = {};

    if (after === 'completado' || after === 'completado_sin_pago') {
      updates[completedPath] = buildCompletedPayload(viaje);
    }

    if (after === 'cancelado') {
      updates[cancelledPath] = buildCancelledPayload(viaje);
    }

    await db.ref().update(updates);

    console.log('✅ Weekly admin sync OK', {
      viajeId,
      status: after,
      weeklyKey,
    });

    return null;
  });

function buildTripPayload(params: {
  type: string;
  viajeId: string;
  title: string;
  body: string;
  targetRole: "cliente" | "conductor";
  channelId: string;
}): PushPayload {
  return {
    type: params.type,
    viajeId: params.viajeId,
    title: params.title,
    body: params.body,
    targetRole: params.targetRole,
    clickAction:
      params.type === "chat_message" ? "OPEN_TRIP_CHAT" : "OPEN_ACTIVE_TRIP",
    channelId: params.channelId,
  };
}

async function getTokens(
  roleNode: PushRole,
  uid: string
): Promise<string[]> {
  const userId = uid.trim();

  if (userId.length === 0) {
    console.log("[getTokens] UID vacío", { roleNode, uid });
    return [];
  }

  const path = `PushTokens/${roleNode}/${userId}`;
  const snap = await db.ref(path).get();

  if (!snap.exists()) {
    console.log("[getTokens] No existen tokens", {
      roleNode,
      userId,
      path,
    });
    return [];
  }

  const value = snap.val() as Record<
    string,
    {
      token?: string;
      active?: boolean;
    }
  >;

  const tokens = new Set<string>();

  Object.values(value).forEach((entry) => {
    const token = asTrimmedString(entry?.token);
    const active = entry?.active !== false;

    if (token.length > 0 && active) {
      tokens.add(token);
    }
  });

  console.log("[getTokens] Tokens encontrados", {
    roleNode,
    userId,
    total: tokens.size,
  });

  return Array.from(tokens);
}

async function removeInvalidTokens(
  roleNode: PushRole,
  uid: string,
  invalidTokens: string[]
): Promise<void> {
  if (invalidTokens.length === 0) {
    return;
  }

  const updates: Record<string, null> = {};
  const uniqueTokens = Array.from(new Set(invalidTokens));

  uniqueTokens.forEach((token) => {
    const tokenId = token.replace(/[.#$[\]/]/g, "_");
    updates[`PushTokens/${roleNode}/${uid}/${tokenId}`] = null;
  });

  await db.ref().update(updates);
}

function chunkArray<T>(items: T[], size: number): T[][] {
  if (items.length === 0) {
    return [];
  }

  const chunks: T[][] = [];
  for (let i = 0; i < items.length; i += size) {
    chunks.push(items.slice(i, i + size));
  }
  return chunks;
}

async function sendPushToUser(
  roleNode: PushRole,
  uid: string,
  payload: PushPayload
): Promise<void> {
  const userId = uid.trim();

  if (userId.length === 0) {
    console.log("[sendPushToUser] UID vacío", { roleNode, payload });
    return;
  }

  const tokens = await getTokens(roleNode, userId);

  if (tokens.length === 0) {
    console.log("[sendPushToUser] Sin tokens para usuario", {
      roleNode,
      userId,
      payload,
    });
    return;
  }

  const invalidTokens: string[] = [];
  const batches = chunkArray(tokens, 500);

  for (const batch of batches) {
    const response = await admin.messaging().sendEachForMulticast({
      tokens: batch,
      notification: {
        title: payload.title,
        body: payload.body,
      },
      data: {
        type: payload.type,
        viajeId: payload.viajeId,
        targetRole: payload.targetRole,
        clickAction: payload.clickAction,
        channelId: payload.channelId,
      },
      android: {
        priority: "high",
        notification: {
          channelId: payload.channelId,
          sound: "default",
        },
      },
      apns: {
        payload: {
          aps: {
            sound: "default",
          },
        },
      },
    });

    console.log("[sendPushToUser] Resultado FCM", {
      roleNode,
      userId,
      type: payload.type,
      successCount: response.successCount,
      failureCount: response.failureCount,
    });

    response.responses.forEach(
      (
        item: {
          success: boolean;
          error?: {
            code?: string;
            message?: string;
          };
        },
        index: number,
      ) => {
        if (item.success) {
          return;
        }

        const code = item.error?.code ?? "";
        console.error("[sendPushToUser] Error FCM", {
          roleNode,
          userId,
          code,
          message: item.error?.message,
        });

        const isInvalid =
          code === "messaging/invalid-registration-token" ||
          code === "messaging/registration-token-not-registered";

        if (isInvalid) {
          invalidTokens.push(batch[index]);
        }
      },
    );
  }

  await removeInvalidTokens(roleNode, userId, invalidTokens);
}

async function getViaje(viajeId: string): Promise<ViajeMap | null> {
  const snap = await db.ref(`TAXI_Viaje/${viajeId}`).get();
  if (!snap.exists()) {
    return null;
  }

  return asMap(snap.val()) as ViajeMap;
}

function toNumber(value: unknown): number {
  if (typeof value === 'number') {
    return Number.isFinite(value) ? value : 0;
  }

  if (typeof value === 'string') {
    const parsed = Number(value.trim());
    return Number.isFinite(parsed) ? parsed : 0;
  }

  return 0;
}

function toInt(value: unknown): number {
  return Math.trunc(toNumber(value));
}

function pesosToCentavos(value: unknown): number {
  return Math.round(toNumber(value) * 100);
}

function readCotizadoCentavos(
  viaje: Record<string, unknown>,
  centavosKey: string,
  pesosKey: string
): number {
  const cotizadoRaw = viaje.cotizado;
  if (!cotizadoRaw || typeof cotizadoRaw !== 'object') {
    return 0;
  }

  const cotizado = asMap(cotizadoRaw);
  const centavos = toInt(cotizado[centavosKey]);
  if (centavos > 0) {
    return centavos;
  }

  const pesos = toNumber(cotizado[pesosKey]);
  if (pesos > 0) {
    return pesosToCentavos(pesos);
  }

  return 0;
}

function buildWalletTxBase(params: {
  descripcion: string;
  tipo: string;
  montoCentavos: number;
  viajeId: string;
  metodoPago: string;
  metodoPagoId: string;
  fecha: object;
}): Record<string, unknown> {
  return {
    descripcion: params.descripcion,
    estatus: 'completado',
    fecha: params.fecha,
    metodoPago: params.metodoPago,
    metodoPagoId: params.metodoPagoId,
    montoCentavos: params.montoCentavos,
    tipo: params.tipo,
    viajeId: params.viajeId,
  };
}

type WalletUpdateReason =
  | 'UPDATED'
  | 'INSUFFICIENT_FUNDS'
  | 'NEGATIVE_RESULT'
  | 'ABORTED'
  | 'EXCEPTION';

type WalletUpdateResult = {
  ok: boolean;
  reason: WalletUpdateReason;
  balanceBefore: number | null;
  balanceAfter: number | null;
  deltaCentavos: number;
  errorMessage?: string;
};

async function updateWalletBalance(
  refPath: string,
  deltaCentavos: number,
  options: { requireSufficientFunds?: boolean } = {}
): Promise<WalletUpdateResult> {
  const ref = db.ref(refPath);

  let balanceBefore: number | null = null;
  let proposedBalance: number | null = null;
  let abortReason: WalletUpdateReason | null = null;

  try {
    const preloadSnap = await ref.get();
    const preloadBalance = toInt(preloadSnap.val());

    const tx = await ref.transaction((current: unknown) => {
      const effectiveCurrent =
        current === null || current === undefined
          ? preloadBalance
          : toInt(current);

      balanceBefore = effectiveCurrent;

      const nextBalance = effectiveCurrent + deltaCentavos;
      proposedBalance = nextBalance;

      if (
        options.requireSufficientFunds === true &&
        deltaCentavos < 0 &&
        effectiveCurrent < Math.abs(deltaCentavos)
      ) {
        abortReason = 'INSUFFICIENT_FUNDS';
        return;
      }

      if (nextBalance < 0 && options.requireSufficientFunds === true) {
        abortReason = 'NEGATIVE_RESULT';
        return;
      }

      return nextBalance;
    });

    const snapshotBalance = tx.snapshot.exists()
      ? toInt(tx.snapshot.val())
      : null;

    if (tx.committed) {
      return {
        ok: true,
        reason: 'UPDATED',
        balanceBefore,
        balanceAfter: snapshotBalance ?? proposedBalance,
        deltaCentavos,
      };
    }

    return {
      ok: false,
      reason: abortReason ?? 'ABORTED',
      balanceBefore,
      balanceAfter: snapshotBalance,
      deltaCentavos,
    };
  } catch (error) {
    return {
      ok: false,
      reason: 'EXCEPTION',
      balanceBefore,
      balanceAfter: null,
      deltaCentavos,
      errorMessage: error instanceof Error ? error.message : String(error),
    };
  }
}

async function markWalletLiquidation(
  viajeId: string,
  payload: Record<string, unknown>
): Promise<void> {
  await db.ref(`TAXI_Viaje/${viajeId}/walletLiquidacion`).update(payload);
}

async function getMaxDeudaConductorCentavos(): Promise<number> {
  const snap = await db.ref('AppConfig/default/maxDeudaConductor').get();
  return Math.max(0, toInt(snap.val()));
}

async function canDriverAcceptTripWithDebtLimit(params: {
  conductorId: string;
  comisionConductorCentavos: number;
}): Promise<{
  ok: boolean;
  walletSaldoActual: number;
  maxDeudaConductorCentavos: number;
  saldoMinimoPermitido: number;
  saldoProyectado: number;
}> {
  const walletRef = `Conductores/${params.conductorId}/walletSaldo`;

  const [walletSnap, maxDebt] = await Promise.all([
    db.ref(walletRef).get(),
    getMaxDeudaConductorCentavos(),
  ]);

  const walletSaldoActual = toInt(walletSnap.val());
  const saldoMinimoPermitido = -maxDebt;
  const saldoProyectado =
    walletSaldoActual - Math.max(0, params.comisionConductorCentavos);

  return {
    ok: saldoProyectado >= saldoMinimoPermitido,
    walletSaldoActual,
    maxDeudaConductorCentavos: maxDebt,
    saldoMinimoPermitido,
    saldoProyectado,
  };
}


// =========================
// Function 1: assignDriverFromOffer
// =========================
export const assignDriverFromOffer = database
  .ref("TAXI_Viaje/{viajeId}/seleccionOferta")
  .onWrite(
    async (
      change: Change<DataSnapshot>,
      context: EventContext
    ): Promise<null> => {
      const viajeId = context.params.viajeId as string;

      const afterVal = change.after.val() as SeleccionOferta | null;
      if (!afterVal || !afterVal.conductorId) {
        return null;
      }

      const conductorId = asTrimmedString(afterVal.conductorId);
      if (conductorId.length === 0) {
        return null;
      }

      const viajeRef = db.ref(`TAXI_Viaje/${viajeId}`);
      const ofertaRef = db.ref(
        `TAXI_OfertasViaje/${viajeId}/ofertas/${conductorId}`
      );

      const ofertaSnap = await ofertaRef.get();
      if (!ofertaSnap.exists()) {
        await viajeRef.child("seleccionOfertaError").set({
          code: "OFFER_NOT_FOUND",
          message: "La oferta seleccionada no existe",
          at: admin.database.ServerValue.TIMESTAMP,
        });
        return null;
      }

      const viajeSnap = await viajeRef.get();
      if (!viajeSnap.exists()) {
        await viajeRef.child('seleccionOfertaError').set({
          code: 'TRIP_NOT_FOUND',
          message: 'El viaje no existe',
          at: admin.database.ServerValue.TIMESTAMP,
        });
        return null;
      }

      const viaje = asMap(viajeSnap.val());
      const metodoPagoId = asTrimmedString(viaje.metodoPagoId).toLowerCase();
      const clienteId = asTrimmedString(viaje.clienteId);

      const totalClienteCentavos =
        readCotizadoCentavos(viaje, 'totalClienteCentavos', 'totalCliente') ||
        pesosToCentavos(viaje.costo);

      const comisionConductorCentavos = readCotizadoCentavos(
        viaje,
        'comisionConductorCentavos',
        'comisionConductor'
      );

      if (metodoPagoId === 'wallet') {
        const clienteWalletSnap = await db
          .ref(`Clientes/${clienteId}/walletSaldo`)
          .get();

        const clienteWalletSaldo = toInt(clienteWalletSnap.val());

        if (clienteWalletSaldo < totalClienteCentavos) {
          await viajeRef.child('seleccionOfertaError').set({
            code: 'CLIENT_WALLET_INSUFFICIENT',
            message: 'El cliente no tiene saldo suficiente en wallet',
            clienteId,
            walletSaldoActual: clienteWalletSaldo,
            totalClienteCentavos,
            at: admin.database.ServerValue.TIMESTAMP,
          });
          return null;
        }
      }

      const debtValidation = await canDriverAcceptTripWithDebtLimit({
        conductorId,
        comisionConductorCentavos,
      });

      if (!debtValidation.ok) {
        await viajeRef.child('seleccionOfertaError').set({
          code: 'DRIVER_MAX_DEBT_REACHED',
          message:
            'El conductor no puede aceptar el viaje porque excede la deuda máxima permitida',
          conductorId,
          walletSaldoActual: debtValidation.walletSaldoActual,
          maxDeudaConductorCentavos:
            debtValidation.maxDeudaConductorCentavos,
          saldoMinimoPermitido: debtValidation.saldoMinimoPermitido,
          saldoProyectado: debtValidation.saldoProyectado,
          comisionConductorCentavos,
          at: admin.database.ServerValue.TIMESTAMP,
        });
        return null;
      }

      await viajeRef.transaction((current: unknown) => {
        if (!current || typeof current !== "object") {
          return current;
        }

        const estatus = asTrimmedString(
          (current as Record<string, unknown>).estatus
        );
        const yaAsignado = asTrimmedString(
          (current as Record<string, unknown>).viajeActualConductorId
        );

        if (estatus !== "solicitud" || yaAsignado.length > 0) {
          return current;
        }

        const next = { ...(current as Record<string, unknown>) };
        const oferta = asMap(ofertaSnap.val());

        next.viajeActualConductorId = conductorId;
        next.estatus = "en_camino";
        next.fechaAceptado = admin.database.ServerValue.TIMESTAMP;
        next.fechaEnCamino = admin.database.ServerValue.TIMESTAMP;

        // Copiar datos relevantes de la oferta elegida al viaje
        next.distancia = oferta.distancia ?? null;
        next.tiempo = oferta.tiempo ?? null;
        next.costo = oferta.costo ?? next.costo ?? null;

        next.ofertaElegida = {
          conductorId,
          ...oferta,
          selectedAt: admin.database.ServerValue.TIMESTAMP,
        };

        next.seleccionOfertaError = null;

        return next;
      });

      return null;
    }
  );

// =========================
// Function 2: validateTripStatusTransition
// Safe: never rewrites "estatus", so no loop.
// =========================
export const validateTripStatusTransition = database
  .ref("TAXI_Viaje/{viajeId}/estatus")
  .onWrite(
    async (
      change: Change<DataSnapshot>,
      context: EventContext
    ): Promise<null> => {
      const beforeRaw = change.before.val();
      const afterRaw = change.after.val();

      const before =
        beforeRaw == null ? null : asTrimmedString(beforeRaw);
      const after =
        afterRaw == null ? null : asTrimmedString(afterRaw);

      if (before === after) {
        return null;
      }

      // No validar eliminación del campo.
      if (after == null || after.length === 0) {
        return null;
      }

      const viajeId = context.params.viajeId as string;
      const viajeRef = db.ref(`TAXI_Viaje/${viajeId}`);

      const ok = canMove(before, after);

      if (!ok) {
        await viajeRef.child("estatusError").set({
          code: "INVALID_TRANSITION",
          message: `Transición inválida: ${before} → ${after}`,
          at: admin.database.ServerValue.TIMESTAMP,
        });
      } else {
        await viajeRef.child("estatusError").set(null);
      }

      return null;
    }
  );

// =========================
// Function 3: notifyOfferAccepted
// Optimized: listens only to the relevant child.
// =========================
export const notifyOfferAccepted = database
  .ref("TAXI_Viaje/{viajeId}/viajeActualConductorId")
  .onWrite(
    async (
      change: Change<DataSnapshot>,
      context: EventContext
    ): Promise<null> => {
      const beforeConductorId = asTrimmedString(change.before.val());
      const afterConductorId = asTrimmedString(change.after.val());

      if (
        beforeConductorId.length > 0 ||
        afterConductorId.length === 0
      ) {
        return null;
      }

      const viajeId = context.params.viajeId as string;

      await sendPushToUser(
        "conductores",
        afterConductorId,
        buildTripPayload({
          type: "offer_accepted",
          viajeId,
          targetRole: "conductor",
          title: "Tu oferta fue aceptada",
          body: "Dirígete al punto de recogida.",
          channelId: "trip_assignments",
        })
      );

      return null;
    }
  );

// =========================
// Function 4: notifyTripStatusChanged
// Optimized: listens only to status changes.
// Never writes to "estatus".
// =========================
export const notifyTripStatusChanged = database
  .ref("TAXI_Viaje/{viajeId}/estatus")
  .onWrite(
    async (
      change: Change<DataSnapshot>,
      context: EventContext
    ): Promise<null> => {
      const beforeStatus = asTrimmedString(change.before.val());
      const afterStatus = asTrimmedString(change.after.val());

      if (beforeStatus === afterStatus || afterStatus.length === 0) {
        return null;
      }

      // No enviar push si la transición es inválida
      const validTransition = canMove(
        beforeStatus.length > 0 ? beforeStatus : null,
        afterStatus
      );

      if (!validTransition) {
        return null;
      }

      const viajeId = context.params.viajeId as string;
      const viaje = await getViaje(viajeId);
      if (!viaje) {
        return null;
      }

      const clienteId = asTrimmedString(viaje.clienteId);
      const conductorId = asTrimmedString(viaje.viajeActualConductorId);
      const canceladoPor = asTrimmedString(viaje.canceladoPor);

      if (afterStatus === "llego" && clienteId.length > 0) {
        await sendPushToUser(
          "clientes",
          clienteId,
          buildTripPayload({
            type: "driver_arrived",
            viajeId,
            targetRole: "cliente",
            title: "Tu conductor ha llegado",
            body: "Ya está en el punto de recogida.",
            channelId: "trip_status",
          })
        );
        return null;
      }

      if (afterStatus === "en_curso" && clienteId.length > 0) {
        await sendPushToUser(
          "clientes",
          clienteId,
          buildTripPayload({
            type: "trip_started",
            viajeId,
            targetRole: "cliente",
            title: "Tu viaje ha comenzado",
            body: "Ya vas en camino a tu destino.",
            channelId: "trip_status",
          })
        );
        return null;
      }

      if (
        (afterStatus === "completado" || afterStatus === "completado_sin_pago") &&
        clienteId.length > 0
      ) {
        await sendPushToUser(
          "clientes",
          clienteId,
          buildTripPayload({
            type: "trip_finished",
            viajeId,
            targetRole: "cliente",
            title: "Tu viaje ha finalizado",
            body: "Gracias por viajar con BeeCar.",
            channelId: "trip_status",
          })
        );
        return null;
      }

      if (afterStatus === "cancelado") {
        if (
          canceladoPor === "CLIENTE" &&
          conductorId.length > 0
        ) {
          await sendPushToUser(
            "conductores",
            conductorId,
            buildTripPayload({
              type: "trip_cancelled_by_client",
              viajeId,
              targetRole: "conductor",
              title: "El cliente canceló el viaje",
              body: "El viaje ya no está activo.",
              channelId: "trip_cancellations",
            })
          );
          return null;
        }

        if (
          canceladoPor === "CONDUCTOR" &&
          clienteId.length > 0
        ) {
          await sendPushToUser(
            "clientes",
            clienteId,
            buildTripPayload({
              type: "trip_cancelled_by_driver",
              viajeId,
              targetRole: "cliente",
              title: "El conductor canceló el viaje",
              body: "Tu viaje fue cancelado.",
              channelId: "trip_cancellations",
            })
          );
          return null;
        }
      }

      return null;
    }
  );

async function getEligibleDriversForTrip(
  viaje: Record<string, unknown>,
): Promise<string[]> {
  const zonaId = asTrimmedString(viaje.zonaId);

  if (!zonaId) {
    console.log("[getEligibleDriversForTrip] Viaje sin zonaId", viaje);
    return [];
  }

  const snap = await db.ref("Conductores")
    .orderByChild("zonaId")
    .equalTo(zonaId)
    .get();

  if (!snap.exists()) {
    console.log("[getEligibleDriversForTrip] Sin conductores en zona", {
      zonaId,
    });
    return [];
  }

  const drivers = snap.val() as Record<string, Record<string, unknown>>;
  const eligibleDriverIds: string[] = [];

  Object.entries(drivers).forEach(([conductorId, conductor]) => {
    const estatus = asTrimmedString(
      conductor.estatus || conductor.estado,
    ).toLowerCase();

    const isActive =
      estatus === "activo" ||
      estatus === "disponible" ||
      estatus === "online";

    if (isActive) {
      eligibleDriverIds.push(conductorId);
    }
  });

  return eligibleDriverIds;
}

export const notifyNewTripRequest = database
  .ref("TAXI_Viaje/{viajeId}")
  .onCreate(
    async (
      snapshot: DataSnapshot,
      context: EventContext,
    ): Promise<null> => {
      const viajeId = context.params.viajeId as string;
      const viaje = asMap(snapshot.val());

      const estatus = asTrimmedString(viaje.estatus).toLowerCase();

      console.log("[notifyNewTripRequest] Viaje creado", {
        viajeId,
        estatus,
        zonaId: viaje.zonaId ?? null,
      });

      if (estatus !== "solicitud") {
        console.log("[notifyNewTripRequest] Ignorado por estatus", {
          viajeId,
          estatus,
        });
        return null;
      }

      const conductorIds = await getEligibleDriversForTrip(viaje);

      console.log("[notifyNewTripRequest] Conductores elegibles", {
        viajeId,
        total: conductorIds.length,
        conductorIds,
      });

      if (conductorIds.length === 0) {
        return null;
      }

      await Promise.all(
        conductorIds.map((conductorId) =>
          sendPushToUser(
            "conductores",
            conductorId,
            buildTripPayload({
              type: "new_trip_request",
              viajeId,
              targetRole: "conductor",
              title: "Nueva solicitud disponible",
              body: "Tienes una nueva solicitud de viaje disponible.",
              channelId: "trip_requests",
            }),
          ),
        ),
      );

      return null;
    },
  );

// =========================
// Function 5: notifyChatMessage
// Safe and minimal.
// =========================
export const notifyChatMessage = database
  .ref("TAXI_ChatViaje/{viajeId}/mensajes/{mensajeId}")
  .onCreate(
    async (
      snapshot: DataSnapshot,
      context: EventContext
    ): Promise<null> => {
      const viajeId = context.params.viajeId as string;
      const message = asMap(snapshot.val());

      const tipo = asTrimmedString(message["tipo"]);
      const texto = asTrimmedString(message["mensaje"]);

      if (tipo.length === 0) {
        return null;
      }

      const viaje = await getViaje(viajeId);
      if (!viaje) {
        return null;
      }

      const clienteId = asTrimmedString(viaje.clienteId);
      const conductorId = asTrimmedString(viaje.viajeActualConductorId);

      const body =
        texto.length > 90 ? `${texto.slice(0, 87)}...` : texto;

      if (tipo === "CLIENTE" && conductorId.length > 0) {
        await sendPushToUser(
          "conductores",
          conductorId,
          buildTripPayload({
            type: "chat_message",
            viajeId,
            targetRole: "conductor",
            title: "Nuevo mensaje",
            body:
              body.length > 0
                ? body
                : "Tienes un nuevo mensaje del cliente.",
            channelId: "chat_messages",
          })
        );
        return null;
      }

      if (tipo === "CONDUCTOR" && clienteId.length > 0) {
        await sendPushToUser(
          "clientes",
          clienteId,
          buildTripPayload({
            type: "chat_message",
            viajeId,
            targetRole: "cliente",
            title: "Nuevo mensaje",
            body:
              body.length > 0
                ? body
                : "Tienes un nuevo mensaje del conductor.",
            channelId: "chat_messages",
          })
        );
        return null;
      }

      return null;
    }
  );


// =========================
// Function 6: liquidateWalletOnTripCompleted
// Applies to wallet and efectivo payments.
// =========================
export const liquidateWalletOnTripCompleted = database
  .ref('TAXI_Viaje/{viajeId}/estatus')
  .onWrite(
    async (
      change: Change<DataSnapshot>,
      context: EventContext
    ): Promise<null> => {
      const beforeStatus = asTrimmedString(change.before.val());
      const afterStatus = asTrimmedString(change.after.val());

      if (beforeStatus === afterStatus || afterStatus !== 'completado') {
        return null;
      }

      const viajeId = context.params.viajeId as string;
      const viajeRef = db.ref(`TAXI_Viaje/${viajeId}`);
      const liquidacionRef = viajeRef.child('walletLiquidacion');

      const lockResult = await liquidacionRef.transaction((current: unknown) => {
        if (current && typeof current === 'object') {
          const currentMap = asMap(current);
          const estado = asTrimmedString(currentMap.estado).toLowerCase();
          if (estado === 'procesando' || estado === 'procesado') {
            return;
          }
        }

        return {
          estado: 'procesando',
          startedAt: admin.database.ServerValue.TIMESTAMP,
        };
      });

      if (!lockResult.committed) {
        return null;
      }

      const viajeRaw = await getViaje(viajeId);
      if (!viajeRaw) {
        await markWalletLiquidation(viajeId, {
          estado: 'error',
          error: 'VIAJE_NO_ENCONTRADO',
          updatedAt: admin.database.ServerValue.TIMESTAMP,
        });
        return null;
      }

      const viaje = asMap(viajeRaw);
      const metodoPagoId = asTrimmedString(viaje.metodoPagoId).toLowerCase();
      const metodoPago = asTrimmedString(viaje.metodoPago);
      const clienteId = asTrimmedString(viaje.clienteId);
      const conductorId = asTrimmedString(
        viaje.viajeActualConductorId ?? viaje.conductorId
      );

      if (clienteId.length === 0 || conductorId.length === 0) {
        await markWalletLiquidation(viajeId, {
          estado: 'error',
          error: 'DATOS_INCOMPLETOS',
          updatedAt: admin.database.ServerValue.TIMESTAMP,
        });
        return null;
      }

      const totalClienteCentavos =
        readCotizadoCentavos(viaje, 'totalClienteCentavos', 'totalCliente') ||
        pesosToCentavos(viaje.costo);

      const totalConductorCentavos = readCotizadoCentavos(
        viaje,
        'totalConductorCentavos',
        'totalConductor'
      );

      const comisionConductorCentavos = readCotizadoCentavos(
        viaje,
        'comisionConductorCentavos',
        'comisionConductor'
      );

      if (metodoPagoId === 'wallet') {
        if (totalClienteCentavos <= 0 || totalConductorCentavos < 0) {
          await markWalletLiquidation(viajeId, {
            estado: 'error',
            error: 'MONTOS_INVALIDOS',
            updatedAt: admin.database.ServerValue.TIMESTAMP,
          });
          return null;
        }

        const clienteWalletRef = `Clientes/${clienteId}/walletSaldo`;
        const conductorWalletRef = `Conductores/${conductorId}/walletSaldo`;

        const clientChargeResult = await updateWalletBalance(
          clienteWalletRef,
          -totalClienteCentavos,
          { requireSufficientFunds: true }
        );

        if (!clientChargeResult.ok) {
          const walletError =
            clientChargeResult.reason === 'INSUFFICIENT_FUNDS'
              ? 'SALDO_INSUFICIENTE_CLIENTE'
              : 'ERROR_CARGO_WALLET_CLIENTE';

          await markWalletLiquidation(viajeId, {
            estado: 'error',
            error: walletError,
            walletErrorReason: clientChargeResult.reason,
            walletErrorDetail: clientChargeResult.errorMessage ?? null,
            updatedAt: admin.database.ServerValue.TIMESTAMP,
          });
          return null;
        }

        const driverCreditResult = await updateWalletBalance(
          conductorWalletRef,
          totalConductorCentavos
        );

        if (!driverCreditResult.ok) {
          await updateWalletBalance(clienteWalletRef, totalClienteCentavos);

          await markWalletLiquidation(viajeId, {
            estado: 'error',
            error: 'NO_SE_PUDO_ABONAR_CONDUCTOR',
            walletErrorReason: driverCreditResult.reason,
            walletErrorDetail: driverCreditResult.errorMessage ?? null,
            updatedAt: admin.database.ServerValue.TIMESTAMP,
          });
          return null;
        }

        const clienteTxRef = db
          .ref(`TransaccionesClientes/${clienteId}/transacciones`)
          .push();
        const conductorAbonoRef = db
          .ref(`TransaccionesConductores/${conductorId}/transacciones`)
          .push();
        const conductorComisionRef = db
          .ref(`TransaccionesConductores/${conductorId}/transacciones`)
          .push();

        const now = admin.database.ServerValue.TIMESTAMP;
        const updates: Record<string, unknown> = {};

        if (clienteTxRef.key) {
          updates[
            `TransaccionesClientes/${clienteId}/transacciones/${clienteTxRef.key}`
          ] = buildWalletTxBase({
            descripcion: 'Pago de viaje',
            tipo: 'cargo',
            montoCentavos: totalClienteCentavos,
            viajeId,
            metodoPago: 'wallet',
            metodoPagoId: 'wallet',
            fecha: now,
          });
        }

        if (conductorAbonoRef.key) {
          updates[
            `TransaccionesConductores/${conductorId}/transacciones/${conductorAbonoRef.key}`
          ] = buildWalletTxBase({
            descripcion: 'Abono por viaje',
            tipo: 'deposito',
            montoCentavos: totalConductorCentavos,
            viajeId,
            metodoPago: 'wallet',
            metodoPagoId: 'wallet',
            fecha: now,
          });
        }

        if (comisionConductorCentavos > 0 && conductorComisionRef.key) {
          updates[
            `TransaccionesConductores/${conductorId}/transacciones/${conductorComisionRef.key}`
          ] = buildWalletTxBase({
            descripcion: 'Comisión de viaje',
            tipo: 'comision_viaje',
            montoCentavos: comisionConductorCentavos,
            viajeId,
            metodoPago: 'wallet',
            metodoPagoId: 'wallet',
            fecha: now,
          });
        }

        await db.ref().update(updates);

        await markWalletLiquidation(viajeId, {
          estado: 'procesado',
          tipoLiquidacion: 'wallet',
          clienteId,
          conductorId,
          totalClienteCentavos,
          totalConductorCentavos,
          comisionConductorCentavos,
          processedAt: admin.database.ServerValue.TIMESTAMP,
          updatedAt: admin.database.ServerValue.TIMESTAMP,
        });

        return null;
      }

      if (metodoPagoId === 'efectivo') {
        if (comisionConductorCentavos < 0) {
          await markWalletLiquidation(viajeId, {
            estado: 'error',
            error: 'COMISION_INVALIDA',
            updatedAt: admin.database.ServerValue.TIMESTAMP,
          });
          return null;
        }

        const conductorWalletRef = `Conductores/${conductorId}/walletSaldo`;

        const debtValidation = await canDriverAcceptTripWithDebtLimit({
          conductorId,
          comisionConductorCentavos,
        });

        if (!debtValidation.ok) {
          await markWalletLiquidation(viajeId, {
            estado: 'error',
            error: 'DRIVER_MAX_DEBT_REACHED',
            walletSaldoActual: debtValidation.walletSaldoActual,
            maxDeudaConductorCentavos:
              debtValidation.maxDeudaConductorCentavos,
            saldoMinimoPermitido: debtValidation.saldoMinimoPermitido,
            saldoProyectado: debtValidation.saldoProyectado,
            updatedAt: admin.database.ServerValue.TIMESTAMP,
          });
          return null;
        }

        const conductorChargeResult = await updateWalletBalance(
          conductorWalletRef,
          -comisionConductorCentavos
        );

        if (!conductorChargeResult.ok) {
          await markWalletLiquidation(viajeId, {
            estado: 'error',
            error: 'ERROR_CARGO_COMISION_CONDUCTOR',
            walletErrorReason: conductorChargeResult.reason,
            walletErrorDetail: conductorChargeResult.errorMessage ?? null,
            updatedAt: admin.database.ServerValue.TIMESTAMP,
          });
          return null;
        }

        const conductorComisionRef = db
          .ref(`TransaccionesConductores/${conductorId}/transacciones`)
          .push();

        if (conductorComisionRef.key) {
          await db
            .ref(
              `TransaccionesConductores/${conductorId}/transacciones/${conductorComisionRef.key}`
            )
            .set(
              buildWalletTxBase({
                descripcion: 'Comisión de viaje',
                tipo: 'comision_viaje',
                montoCentavos: comisionConductorCentavos,
                viajeId,
                metodoPago: metodoPago.length > 0 ? metodoPago : 'Efectivo',
                metodoPagoId: 'efectivo',
                fecha: admin.database.ServerValue.TIMESTAMP,
              })
            );
        }

        await markWalletLiquidation(viajeId, {
          estado: 'procesado',
          tipoLiquidacion: 'efectivo',
          clienteId,
          conductorId,
          comisionConductorCentavos,
          processedAt: admin.database.ServerValue.TIMESTAMP,
          updatedAt: admin.database.ServerValue.TIMESTAMP,
        });

        return null;
      }

      await markWalletLiquidation(viajeId, {
        estado: 'no_aplica',
        tipoLiquidacion: metodoPagoId,
        updatedAt: admin.database.ServerValue.TIMESTAMP,
      });

      return null;
    }
  );


function sanitizeRatingValue(value: unknown): number | null {
  const parsed = Number((value ?? "").toString().trim());
  if (!Number.isFinite(parsed)) {
    return null;
  }

  const normalized = Math.trunc(parsed);
  if (normalized < 1 || normalized > 5) {
    return null;
  }

  return normalized;
}

async function recalculateAverageRating(params: {
  ratingsPath: string;
  profilePath: string;
}): Promise<void> {
  const ratingsSnap = await db.ref(params.ratingsPath).get();
  const ratingsRaw = ratingsSnap.val();

  if (!ratingsRaw || typeof ratingsRaw !== "object") {
    await db.ref(params.profilePath).update({
      calificacion: 0,
    });
    return;
  }

  const ratingsMap = asMap(ratingsRaw);

  let sum = 0;
  let count = 0;

  Object.values(ratingsMap).forEach((item) => {
    const ratingMap = asMap(item);
    const rating = sanitizeRatingValue(ratingMap.calificacion);

    if (rating == null) {
      return;
    }

    sum += rating;
    count += 1;
  });

  const average = count > 0 ? Number((sum / count).toFixed(1)) : 0;

  await db.ref(params.profilePath).update({
    calificacion: average,
  });
}
export const markTripRatingPendingOnCompleted = database
  .ref("TAXI_Viaje/{viajeId}/estatus")
  .onWrite(
    async (
      change: Change<DataSnapshot>,
      context: EventContext
    ): Promise<null> => {
      const beforeStatus = asTrimmedString(change.before.val()).toLowerCase();
      const afterStatus = asTrimmedString(change.after.val()).toLowerCase();

      if (beforeStatus === afterStatus || afterStatus !== "completado") {
        return null;
      }

      const viajeId = context.params.viajeId as string;
      const viajeSnap = await db.ref(`TAXI_Viaje/${viajeId}`).get();

      if (!viajeSnap.exists()) {
        return null;
      }

      const viaje = asMap(viajeSnap.val());

      const clienteId = asTrimmedString(viaje.clienteId);
      const conductorId = asTrimmedString(
        viaje.viajeActualConductorId ?? viaje.conductorId
      );

      const updates: Record<string, unknown> = {};

      if (clienteId.length > 0) {
        updates.califPendClienteId = clienteId;
      }

      if (conductorId.length > 0) {
        updates.califPendConductorId = conductorId;
      }

      if (Object.keys(updates).length === 0) {
        return null;
      }

      await db.ref(`TAXI_Viaje/${viajeId}`).update(updates);

      console.log("✅ Pending ratings marked", {
        viajeId,
        clienteId,
        conductorId,
      });

      return null;
    }
  );
export const recalculateDriverRating = database
  .ref("ConductoresCalificaciones/{conductorId}/calificaciones/{viajeId}")
  .onWrite(
    async (
      change: Change<DataSnapshot>,
      context: EventContext
    ): Promise<null> => {
      const conductorId = context.params.conductorId as string;

      await recalculateAverageRating({
        ratingsPath: `ConductoresCalificaciones/${conductorId}/calificaciones`,
        profilePath: `Conductores/${conductorId}`,
      });

      console.log("✅ Driver rating recalculated", { conductorId });

      return null;
    }
  );
export const recalculateClientRating = database
  .ref("ClientesCalificaciones/{clienteId}/calificaciones/{viajeId}")
  .onWrite(
    async (
      change: Change<DataSnapshot>,
      context: EventContext
    ): Promise<null> => {
      const clienteId = context.params.clienteId as string;

      await recalculateAverageRating({
        ratingsPath: `ClientesCalificaciones/${clienteId}/calificaciones`,
        profilePath: `Clientes/${clienteId}`,
      });

      console.log("✅ Client rating recalculated", { clienteId });

      return null;
    }
  );
